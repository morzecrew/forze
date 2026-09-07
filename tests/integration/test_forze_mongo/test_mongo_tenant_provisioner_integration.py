"""Mongo database tenant provisioner against a live ``mongod``.

The provisioner's claims are all claims about a server: that a database MongoDB creates
lazily is really there afterwards, that a re-run does not restamp anything, that concurrent
onboardings of one tenant settle on a single marker, that an offboarding drops what it owns
and refuses what it does not, and that a connection user without rights on the tenant's
database fails at onboarding rather than at a customer's first request. None of those can be
established against a fake client, so none of them are tested against one.

# covers: MongoDatabaseTenantProvisioner.provision
# covers: MongoDatabaseTenantProvisioner.deprovision
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, cast

import pytest
import pytest_asyncio

pytest.importorskip("pymongo")
pytest.importorskip("testcontainers.mongodb")

from testcontainers.mongodb import MongoDbContainer

from forze.application.contracts.tenancy import TenantIdentity
from forze.base.exceptions import CoreException
from forze.base.primitives import utcnow
from forze_mongo.adapters.tenant_provisioner import MongoDatabaseTenantProvisioner
from forze_mongo.kernel.client.client import MongoClient
from forze_mongo.kernel.uri import with_mongo_credentials

# ----------------------- #

pytestmark = pytest.mark.integration

_LOCK = "_forze_tenant_offboarding"
_MARKER = "_forze_tenants"
"""Spelled out rather than imported: it is the name written to a live deployment, so a rename
has to fail here and be decided, not follow the source silently."""


def _tenant() -> TenantIdentity:
    return TenantIdentity(tenant_id=uuid.uuid4())


def _database(dropper: list[str], prefix: str) -> str:
    """Name a fresh database and register it for removal.

    Unique per call because these tests create real databases beside the client fixture's own,
    and registered as it is named because several tests here end with the database still
    standing — a refused drop is the assertion.
    """

    name = f"forze_tp_{prefix}_{uuid.uuid4().hex[:8]}"
    dropper.append(name)

    return name


def _provisioner(
    client: MongoClient,
    database: str,
    *,
    drop_on_deprovision: bool = False,
) -> MongoDatabaseTenantProvisioner:
    """A provisioner resolving every tenant to *database* — the constant resolver on purpose.

    It is what the per-tenant deployment looks like from one tenant's side, and what a
    misconfigured deployment looks like from two.
    """

    return MongoDatabaseTenantProvisioner(
        client=client,
        database=lambda _: database,
        drop_on_deprovision=drop_on_deprovision,
    )


@pytest_asyncio.fixture
async def dropper(mongo_client: MongoClient) -> AsyncIterator[list[str]]:
    """Databases to remove once the test is done; see :func:`_database`."""

    created: list[str] = []

    yield created

    handle = await mongo_client.db()

    for name in created:
        await handle.client.drop_database(name)


async def _database_names(client: MongoClient) -> list[str]:
    handle = await client.db()

    return await handle.client.list_database_names()


async def _markers(client: MongoClient, database: str) -> list[dict]:
    coll = await client.collection(_MARKER, db_name=database)

    return await client.find_many(coll, {})


async def _locks(client: MongoClient) -> list[dict]:
    """Offboarding locks, read from where the provisioner keeps them by default."""

    coll = await client.collection(_LOCK)

    return await client.find_many(coll, {})


@asynccontextmanager
async def _teardown_stalled_after_its_read(
    client: MongoClient,
    tenant: TenantIdentity,
    database: str,
) -> AsyncIterator[None]:
    """Run a teardown of *database* and hold it open at the ownership read.

    The body runs inside the window; leaving it lets the teardown finish. Resumed from a
    ``finally`` so a failing assertion inside the window cannot strand the task holding the
    lock — a stalled teardown outliving its test takes the client down with it and buries the
    real failure under a shutdown error.
    """

    read, resume = asyncio.Event(), asyncio.Event()
    stalled = _provisioner(
        cast(MongoClient, _StallsTheOwnershipRead(client, read, resume)),
        database,
        drop_on_deprovision=True,
    )
    teardown = asyncio.create_task(stalled.deprovision(tenant))

    try:
        await asyncio.wait_for(read.wait(), timeout=10)
        yield

    finally:
        resume.set()
        await asyncio.wait_for(teardown, timeout=10)


class _StallsTheOwnershipRead:
    """A client that holds the teardown open at the exact moment the window used to be.

    Everything is delegated to the real client except the ownership read, which signals once
    it has answered and then waits — so a test can drive an onboarding into the gap between
    that read and the ``dropDatabase`` deliberately, rather than hoping a scheduler produces
    it.
    """

    def __init__(self, inner: MongoClient, read: asyncio.Event, resume: asyncio.Event) -> None:
        self._inner = inner
        self._read = read
        self._resume = resume

    def __getattr__(self, item: str) -> Any:
        return getattr(self._inner, item)

    async def find_many(self, *args: Any, **kwargs: Any) -> list[Any]:
        found = await self._inner.find_many(*args, **kwargs)
        self._read.set()
        await self._resume.wait()

        return found


class _CannotRelease:
    """A client whose deletes fail, so the lock survives the teardown that took it."""

    def __init__(self, inner: MongoClient) -> None:
        self._inner = inner

    def __getattr__(self, item: str) -> Any:
        return getattr(self._inner, item)

    async def delete_one(self, *args: Any, **kwargs: Any) -> int:
        raise RuntimeError("the release could not be written")


# ....................... #


class TestProvision:
    @pytest.mark.asyncio
    async def test_the_database_exists_afterwards(
        self,
        mongo_client: MongoClient,
        dropper: list[str],
    ) -> None:
        """MongoDB has no empty database: one shows up in ``listDatabases`` only once
        something is written to it. Provisioning is what writes, so the tenant's container is
        real — and visible to an operator listing what is onboarded — before any request."""

        tenant, name = _tenant(), _database(dropper, "exists")

        assert name not in await _database_names(mongo_client)

        await _provisioner(mongo_client, name).provision(tenant)

        assert name in await _database_names(mongo_client)

        markers = await _markers(mongo_client, name)
        assert [doc["_id"] for doc in markers] == [str(tenant.tenant_id)]
        assert markers[0]["tenant_id"] == str(tenant.tenant_id)

    @pytest.mark.asyncio
    async def test_a_rerun_leaves_the_first_onboarding_stamp_alone(
        self,
        mongo_client: MongoClient,
        dropper: list[str],
    ) -> None:
        """Onboarding is retried after partial failure and re-run by operators, so idempotent
        has to mean more than "does not error": ``provisioned_at`` is the field somebody reads
        to answer when a tenant joined, and a re-run that moved it would erase that answer."""

        tenant, name = _tenant(), _database(dropper, "rerun")

        provisioner = _provisioner(mongo_client, name)

        await provisioner.provision(tenant)
        first = (await _markers(mongo_client, name))[0]["provisioned_at"]

        await asyncio.sleep(0.01)
        await provisioner.provision(tenant)

        assert (await _markers(mongo_client, name))[0]["provisioned_at"] == first
        assert len(await _markers(mongo_client, name)) == 1

    @pytest.mark.asyncio
    async def test_concurrent_onboardings_of_one_tenant_settle_on_one_marker(
        self,
        mongo_client: MongoClient,
        dropper: list[str],
    ) -> None:
        """The reason ``provision`` catches nothing.

        An upsert is check-then-act inside the server, so onboardings that all find nothing
        all insert — a retried onboarding, or two workers handed the same event, is exactly
        that ordering. What keeps it from surfacing as a duplicate key is that the predicate
        *is* the unique index, which mongod retries internally. This is the standing check on
        that: sixteen onboardings released together must leave one marker, and if a server
        ever stops converging them this is what says so.
        """

        tenant, name = _tenant(), _database(dropper, "race")

        provisioner = _provisioner(mongo_client, name)

        await asyncio.gather(*(provisioner.provision(tenant) for _ in range(16)))

        assert len(await _markers(mongo_client, name)) == 1

    @pytest.mark.asyncio
    async def test_two_tenants_sharing_a_name_each_get_their_own_marker(
        self,
        mongo_client: MongoClient,
        dropper: list[str],
    ) -> None:
        """A constant resolver is a legal thing to write and passes every construction check.
        Provisioning does not refuse it — a shared database with per-tenant collections is a
        real deployment — it records who is in there, which is what teardown reads."""

        first, second, name = _tenant(), _tenant(), _database(dropper, "shared")

        provisioner = _provisioner(mongo_client, name)

        await provisioner.provision(first)
        await provisioner.provision(second)

        assert {doc["_id"] for doc in await _markers(mongo_client, name)} == {
            str(first.tenant_id),
            str(second.tenant_id),
        }

    @pytest.mark.asyncio
    async def test_a_connection_that_cannot_write_the_tenant_fails_at_onboarding(
        self,
        mongo_container: MongoDbContainer,
        mongo_client: MongoClient,
        dropper: list[str],
    ) -> None:
        """The headline reason this class exists on a server that creates databases lazily.

        Without provisioning, a connection user lacking rights on the tenant's database is
        discovered by a customer's first request — at runtime, one request at a time, long
        after onboarding reported success. The marker write is a real write to the tenant's
        own database, so the same missing grant fails the onboarding instead, where it can be
        fixed and retried.
        """

        name = _database(dropper, "denied")

        user = f"forze_tp_{uuid.uuid4().hex[:8]}"
        admin = (await mongo_client.db()).client["admin"]

        # Can authenticate, and can read one unrelated database — everything except write the
        # tenant's. A user with no roles at all would fail for a less specific reason.
        await admin.command(
            "createUser",
            user,
            pwd="pw",
            roles=[{"role": "read", "db": "forze_tp_elsewhere"}],
        )

        try:
            uri = with_mongo_credentials(
                mongo_container.get_connection_url(),
                username=user,
                password="pw",
            )
            confined = MongoClient()
            await confined.initialize(uri, db_name=name)

            try:
                with pytest.raises(CoreException) as caught:
                    await _provisioner(confined, name).provision(_tenant())

                # Named, not merely "something raised": a provision that failed for an
                # unrelated reason — an unreachable server, a bad URI — would satisfy a bare
                # `raises(CoreException)` while proving nothing about the missing grant.
                assert "authorization" in str(caught.value).lower()

            finally:
                await confined.close()

        finally:
            await admin.command("dropUser", user)

        assert name not in await _database_names(mongo_client)


class TestDeprovision:
    @pytest.mark.asyncio
    async def test_the_database_is_dropped_when_asked(
        self,
        mongo_client: MongoClient,
        dropper: list[str],
    ) -> None:
        tenant, name = _tenant(), _database(dropper, "drop")

        provisioner = _provisioner(mongo_client, name, drop_on_deprovision=True)

        await provisioner.provision(tenant)
        coll = await mongo_client.collection("orders", db_name=name)
        await mongo_client.insert_one(coll, {"total": 1})

        await provisioner.deprovision(tenant)

        assert name not in await _database_names(mongo_client)

    @pytest.mark.asyncio
    async def test_teardown_is_a_no_op_unless_it_was_asked_for(
        self,
        mongo_client: MongoClient,
        dropper: list[str],
    ) -> None:
        """Deleting a tenant's data is never the default: an offboarding that only forgets the
        tenant is recoverable, and one that destroyed the database is not."""

        tenant, name = _tenant(), _database(dropper, "keep")

        provisioner = _provisioner(mongo_client, name)

        await provisioner.provision(tenant)
        await provisioner.deprovision(tenant)

        assert name in await _database_names(mongo_client)

    @pytest.mark.asyncio
    async def test_offboarding_an_absent_database_is_quiet(
        self,
        mongo_client: MongoClient,
        dropper: list[str],
    ) -> None:
        """Teardown is re-run at least as often as onboarding — a half-finished cleanup job, a
        retried command, an operator repeating themselves. The second pass has nothing to drop
        and nothing to complain about."""

        tenant, name = _tenant(), _database(dropper, "absent")

        provisioner = _provisioner(mongo_client, name, drop_on_deprovision=True)

        await provisioner.provision(tenant)
        await provisioner.deprovision(tenant)
        await provisioner.deprovision(tenant)

        assert name not in await _database_names(mongo_client)

    @pytest.mark.asyncio
    async def test_a_database_holding_another_tenant_is_refused(
        self,
        mongo_client: MongoClient,
        dropper: list[str],
    ) -> None:
        """Where a resolver that only *looks* per-tenant is finally caught. Nothing about
        ``lambda _: "app"`` can be checked at construction — the tenant ids do not exist yet
        and it may be async — so the collision is read off the server at the moment the drop
        would destroy the other tenant, and the other tenant is named."""

        first, second, name = _tenant(), _tenant(), _database(dropper, "cotenant")

        provisioner = _provisioner(mongo_client, name, drop_on_deprovision=True)

        await provisioner.provision(first)
        await provisioner.provision(second)

        with pytest.raises(CoreException, match=str(second.tenant_id)) as caught:
            await provisioner.deprovision(first)

        assert caught.value.code == "tenant_database_shared_across_tenants"
        assert name in await _database_names(mongo_client)

    @pytest.mark.asyncio
    async def test_an_unprovisioned_database_is_refused(
        self,
        mongo_client: MongoClient,
        dropper: list[str],
    ) -> None:
        """A resolver produced a name; the name reached something with data in it that this
        provisioner never registered. That is what a typo looks like from here, and the
        difference between it and a legitimate offboarding is not visible in the name."""

        tenant, name = _tenant(), _database(dropper, "foreign")

        coll = await mongo_client.collection("payroll", db_name=name)
        await mongo_client.insert_one(coll, {"amount": 1})

        provisioner = _provisioner(mongo_client, name, drop_on_deprovision=True)

        with pytest.raises(CoreException) as caught:
            await provisioner.deprovision(tenant)

        assert caught.value.code == "tenant_database_not_provisioned"
        assert await mongo_client.count(coll, {}) == 1

    @pytest.mark.asyncio
    @pytest.mark.parametrize("damage", ["foreign_identity", "nothing_but_the_id", "no_stamp"])
    async def test_a_marker_this_provisioner_did_not_write_is_refused(
        self,
        mongo_client: MongoClient,
        dropper: list[str],
        damage: str,
    ) -> None:
        """The document authorizing the drop has to be recognisable, not merely present.

        ``_id`` alone is a name anyone can write. A document carrying this tenant's id but
        not the fields ``provision`` writes is either somebody else's record or a corrupted
        one, and both readings say the same thing: what is in this database is unknown here.
        Treating an unreadable marker as ownership is how a lenient branch for *missing*
        state ends up swallowing *corrupt* state — with a ``dropDatabase`` behind it.

        Every field the marker carries is checked, not the first one: half a marker is not a
        marker, and a document three-quarters right is likelier to be a collision with
        something else than a document that shares only its name.
        """

        tenant, name = _tenant(), _database(dropper, "malformed")
        planted: dict[str, object] = {
            "foreign_identity": {"tenant_id": "someone-else", "provisioned_at": utcnow()},
            "nothing_but_the_id": {},
            "no_stamp": {"tenant_id": str(tenant.tenant_id)},
        }[damage]

        coll = await mongo_client.collection(_MARKER, db_name=name)
        await mongo_client.insert_one(coll, {"_id": str(tenant.tenant_id), **planted})

        with pytest.raises(CoreException) as caught:
            await _provisioner(mongo_client, name, drop_on_deprovision=True).deprovision(tenant)

        assert caught.value.code == "tenant_marker_unrecognized"
        assert name in await _database_names(mongo_client)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("stamp", [{"provisioned_at": "old"}, {}])
    async def test_a_damaged_marker_is_repaired_by_the_reprovision_the_refusal_asks_for(
        self,
        mongo_client: MongoClient,
        dropper: list[str],
        stamp: dict,
    ) -> None:
        """The recovery the refusal names has to work, or the refusal strands the database.

        It is what the marker upsert is a pipeline for. The identity is rewritten every pass,
        so a re-run repairs it rather than matching the document and changing nothing; the
        onboarding stamp is filled only where it is absent, because a re-run that moved it
        would erase the answer to when the tenant joined. ``$setOnInsert`` can do the second
        and not the first, which is why neither field uses it.
        """

        tenant, name = _tenant(), _database(dropper, "repair")
        coll = await mongo_client.collection(_MARKER, db_name=name)
        await mongo_client.insert_one(
            coll,
            {"_id": str(tenant.tenant_id), "tenant_id": "someone-else", **stamp},
        )

        provisioner = _provisioner(mongo_client, name, drop_on_deprovision=True)
        await provisioner.provision(tenant)

        repaired = (await _markers(mongo_client, name))[0]
        assert repaired["tenant_id"] == str(tenant.tenant_id)
        assert repaired["provisioned_at"] == stamp.get("provisioned_at", repaired["provisioned_at"])
        assert repaired["provisioned_at"] is not None

        await provisioner.deprovision(tenant)

        assert name not in await _database_names(mongo_client)

    @pytest.mark.asyncio
    async def test_the_refused_database_can_be_offboarded_after_provisioning(
        self,
        mongo_client: MongoClient,
        dropper: list[str],
    ) -> None:
        """The recovery the refusal names has to actually work, or the refusal is a dead end
        for a database that really was the tenant's — provisioned by hand before this class
        was wired in."""

        tenant, name = _tenant(), _database(dropper, "adopt")

        coll = await mongo_client.collection("payroll", db_name=name)
        await mongo_client.insert_one(coll, {"amount": 1})

        provisioner = _provisioner(mongo_client, name, drop_on_deprovision=True)

        await provisioner.provision(tenant)
        await provisioner.deprovision(tenant)

        assert name not in await _database_names(mongo_client)


class TestOffboardingLock:
    """The window between the ownership read and the drop, and what now stands in it."""

    @pytest.mark.asyncio
    async def test_an_onboarding_inside_the_teardown_window_never_returns_success(
        self,
        mongo_client: MongoClient,
        dropper: list[str],
    ) -> None:
        """The race itself, driven rather than waited for.

        Two tenants resolving to one database — the misconfiguration teardown refuses under
        every other schedule — with the second onboarding landing in the one gap where the
        refusal cannot see it: after the ownership read, before the drop. What must not happen
        is the pair *onboarding reported success* and *its database was dropped*; either
        outcome alone is fine, and an onboarding told to retry is the one taken here.
        """

        leaving, arriving, name = _tenant(), _tenant(), _database(dropper, "window")

        await _provisioner(mongo_client, name).provision(leaving)

        async with _teardown_stalled_after_its_read(mongo_client, leaving, name):
            with pytest.raises(CoreException) as caught:
                await _provisioner(mongo_client, name).provision(arriving)

        assert caught.value.code == "tenant_offboarding_in_flight"
        assert name not in await _database_names(mongo_client)

    @pytest.mark.asyncio
    async def test_the_lock_is_gone_once_the_teardown_is(
        self,
        mongo_client: MongoClient,
        dropper: list[str],
    ) -> None:
        """It is not the drop that releases it — the lock lives outside the database being
        dropped, which is the whole point — so the release is a write that has to happen."""

        tenant, name = _tenant(), _database(dropper, "released")
        provisioner = _provisioner(mongo_client, name, drop_on_deprovision=True)

        await provisioner.provision(tenant)
        await provisioner.deprovision(tenant)

        assert [doc["_id"] for doc in await _locks(mongo_client)] == []

        # And onboarding the same name again is not blocked by what the teardown left.
        await provisioner.provision(tenant)

        assert name in await _database_names(mongo_client)

    @pytest.mark.asyncio
    async def test_a_second_teardown_of_one_database_is_refused_not_queued(
        self,
        mongo_client: MongoClient,
        dropper: list[str],
    ) -> None:
        """Two overlapping ``dropDatabase`` calls have no ordering worth waiting for, and a
        lock left behind by a dead holder is indistinguishable from a live one — so the
        refusal names the document to remove rather than blocking on it."""

        tenant, name = _tenant(), _database(dropper, "contended")
        provisioner = _provisioner(mongo_client, name, drop_on_deprovision=True)

        await provisioner.provision(tenant)

        locks = await mongo_client.collection(_LOCK)
        await mongo_client.insert_one(locks, {"_id": name, "tenant_id": "someone", "at": utcnow()})

        with pytest.raises(CoreException) as caught:
            await provisioner.deprovision(tenant)

        assert caught.value.code == "tenant_offboarding_in_flight"
        assert name in await _database_names(mongo_client)

        # The refusal left the other holder's lock exactly as it found it: a teardown that
        # released a lock it never took would open the window for whoever does hold it.
        assert [doc["tenant_id"] for doc in await _locks(mongo_client)] == ["someone"]

    @pytest.mark.asyncio
    async def test_the_lock_records_who_is_leaving_and_when(
        self,
        mongo_client: MongoClient,
        dropper: list[str],
    ) -> None:
        """A stuck lock is cleared by hand, so it has to say enough for someone to decide
        whether clearing it is safe: which tenant took it, and how long ago."""

        tenant, name = _tenant(), _database(dropper, "contents")

        await _provisioner(mongo_client, name).provision(tenant)

        async with _teardown_stalled_after_its_read(mongo_client, tenant, name):
            held = await _locks(mongo_client)

        assert [doc["_id"] for doc in held] == [name]
        assert held[0]["tenant_id"] == str(tenant.tenant_id)
        assert isinstance(held[0]["at"], datetime)

    @pytest.mark.asyncio
    async def test_a_lock_inside_the_database_being_dropped_is_refused(
        self,
        mongo_client: MongoClient,
        dropper: list[str],
    ) -> None:
        """A lock the teardown destroys is not a lock. It would be released by the very drop
        it was holding open, and the window would stand open exactly while it was in use."""

        tenant, name = _tenant(), _database(dropper, "selflock")
        provisioner = MongoDatabaseTenantProvisioner(
            client=mongo_client,
            database=lambda _: name,
            drop_on_deprovision=True,
            lock_database=name,
        )

        # Refused at both ends, because the onboarding reads the same misplaced collection.
        for call in (provisioner.provision, provisioner.deprovision):
            with pytest.raises(CoreException) as caught:
                await call(tenant)

            assert caught.value.code == "tenant_lock_inside_target_database"

    @pytest.mark.asyncio
    async def test_a_release_that_fails_after_a_successful_drop_is_reported(
        self,
        mongo_client: MongoClient,
        dropper: list[str],
    ) -> None:
        """The drop is durable, so this error outranks nothing — and nothing else would tell
        the caller that the database name is now wedged for every future onboarding."""

        tenant, name = _tenant(), _database(dropper, "stuck")

        await _provisioner(mongo_client, name).provision(tenant)

        broken = _provisioner(
            cast(MongoClient, _CannotRelease(mongo_client)),
            name,
            drop_on_deprovision=True,
        )

        with pytest.raises(CoreException) as caught:
            await broken.deprovision(tenant)

        assert caught.value.code == "tenant_offboarding_lock_stuck"
        assert name not in await _database_names(mongo_client)
        assert [doc["_id"] for doc in await _locks(mongo_client)] == [name]

    @pytest.mark.asyncio
    async def test_a_release_that_fails_does_not_replace_the_teardowns_own_error(
        self,
        mongo_client: MongoClient,
        dropper: list[str],
    ) -> None:
        """The refusal is what the caller has to act on — a co-tenant's data is about to be
        destroyed — and a bookkeeping write failing on the way out must not be what they read
        instead. A stuck lock is the lesser of the two facts, so it goes to the log."""

        first, second, name = _tenant(), _tenant(), _database(dropper, "outranked")
        keeper = _provisioner(mongo_client, name)

        await keeper.provision(first)
        await keeper.provision(second)

        broken = _provisioner(
            cast(MongoClient, _CannotRelease(mongo_client)),
            name,
            drop_on_deprovision=True,
        )

        with pytest.raises(CoreException) as caught:
            await broken.deprovision(first)

        assert caught.value.code == "tenant_database_shared_across_tenants"
        assert name in await _database_names(mongo_client)
