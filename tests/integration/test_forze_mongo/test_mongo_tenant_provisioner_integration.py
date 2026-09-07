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
from datetime import datetime, timedelta
from typing import Any, cast

import pytest
import pytest_asyncio

pytest.importorskip("pymongo")
pytest.importorskip("testcontainers.mongodb")

from pymongo import ReadPreference
from testcontainers.mongodb import MongoDbContainer

from forze.application.contracts.tenancy import TenantIdentity
from forze.base.exceptions import CoreException, ExceptionKind
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


class _Wraps:
    """A real client with one call changed, so a test drives the interleaving rather than
    hoping a scheduler produces it. Everything not overridden is the server's own behaviour."""

    def __init__(self, inner: MongoClient) -> None:
        self._inner = inner

    def __getattr__(self, item: str) -> Any:
        return getattr(self._inner, item)


class _StallsTheOwnershipRead(_Wraps):
    """Holds a teardown open where its window is: after the read that authorises the drop and
    before the drop itself."""

    def __init__(self, inner: MongoClient, read: asyncio.Event, resume: asyncio.Event) -> None:
        super().__init__(inner)
        self._read = read
        self._resume = resume

    async def find_many(self, *args: Any, **kwargs: Any) -> list[Any]:
        found = await self._inner.find_many(*args, **kwargs)
        self._read.set()
        await self._resume.wait()

        return found


class _StallsAfterTheMarkerWrite(_Wraps):
    """Holds an onboarding open between writing its marker and looking at anything.

    The other half of the window, and the one an ordering argument gets wrong first: an
    onboarding can read the lock *after* a teardown has already released it, having written
    its marker while that teardown was mid-drop.
    """

    def __init__(self, inner: MongoClient, wrote: asyncio.Event, resume: asyncio.Event) -> None:
        super().__init__(inner)
        self._wrote = wrote
        self._resume = resume

    async def update_one_upsert(self, *args: Any, **kwargs: Any) -> Any:
        result = await self._inner.update_one_upsert(*args, **kwargs)
        self._wrote.set()
        await self._resume.wait()

        return result


class _StallsAfterTheFirstRead(_Wraps):
    """Holds an onboarding open between its two reads, and lets the rest through.

    Which read comes first is not arbitrary, and nothing else here can tell the two orders
    apart: both refuse an onboarding that is merely *inside* a teardown's window. They differ
    only for one that is interrupted between the reads while the teardown finishes.
    """

    def __init__(self, inner: MongoClient, read: asyncio.Event, resume: asyncio.Event) -> None:
        super().__init__(inner)
        self._read = read
        self._resume = resume
        self._stalled = False

    async def find_one(self, *args: Any, **kwargs: Any) -> Any:
        found = await self._inner.find_one(*args, **kwargs)

        if not self._stalled:
            self._stalled = True
            self._read.set()
            await self._resume.wait()

        return found


class _RecordsReadPreferences(_Wraps):
    """Captures the read preference of every collection the protocol actually reads from."""

    def __init__(self, inner: MongoClient) -> None:
        super().__init__(inner)
        self.preferences: list[Any] = []
        self.metadata_preferences: list[Any] = []

    async def find_one(self, coll: Any, *args: Any, **kwargs: Any) -> Any:
        self.preferences.append(coll.read_preference)

        return await self._inner.find_one(coll, *args, **kwargs)

    async def find_many(self, coll: Any, *args: Any, **kwargs: Any) -> list[Any]:
        self.preferences.append(coll.read_preference)

        return await self._inner.find_many(coll, *args, **kwargs)

    async def db(self, name: str | None = None) -> Any:
        return _RecordingDatabase(await self._inner.db(name), self.metadata_preferences)


class _RecordingDatabase:
    """A database handle that records the read preference its metadata read actually ran at.

    ``with_options`` returns another of these rather than the plain handle, because the pin
    the provisioner applies happens *after* it takes the handle — a recorder that stopped at
    the first hop would report the connection's preference and call the pin proven.
    """

    def __init__(self, inner: Any, sink: list[Any]) -> None:
        self._inner = inner
        self._sink = sink

    def __getattr__(self, item: str) -> Any:
        return getattr(self._inner, item)

    def with_options(self, *args: Any, **kwargs: Any) -> _RecordingDatabase:
        return _RecordingDatabase(self._inner.with_options(*args, **kwargs), self._sink)

    async def list_collection_names(self, *args: Any, **kwargs: Any) -> list[str]:
        self._sink.append(self._inner.read_preference)

        return await self._inner.list_collection_names(*args, **kwargs)


class _DropFails(_Wraps):
    """A client whose ``dropDatabase`` raises without saying whether the server took it."""

    async def db(self, name: str | None = None) -> Any:
        return _RefusesCommands(await self._inner.db(name))


class _RefusesCommands:
    def __init__(self, inner: Any) -> None:
        self._inner = inner

    def __getattr__(self, item: str) -> Any:
        return getattr(self._inner, item)

    async def command(self, *args: Any, **kwargs: Any) -> Any:
        raise RuntimeError("the drop's answer never came back")


class _CannotRelease(_Wraps):
    """A client whose deletes fail, so the lock survives the teardown that took it."""

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
        assert caught.value.kind is ExceptionKind.CONCURRENCY
        assert name not in await _database_names(mongo_client)

    @pytest.mark.asyncio
    async def test_an_onboarding_that_the_whole_teardown_outran_is_refused(
        self,
        mongo_client: MongoClient,
        dropper: list[str],
    ) -> None:
        """The interleaving a lock read alone does not catch.

        Reading the lock and finding none has two readings, not one: the teardown has not
        started, or it has already finished. In the second, the onboarding wrote its marker
        while the teardown was between its ownership read and its drop — so the drop took the
        marker, the release cleared the lock, and only then did the onboarding look. It would
        report success having written nothing that still exists.

        So the onboarding also reads back the marker it just wrote. Both checks are needed and
        neither is redundant: this ordering passes the lock read, and an onboarding that lands
        squarely inside the window fails the lock read while its marker is still there.
        """

        leaving, arriving, name = _tenant(), _tenant(), _database(dropper, "outran")
        wrote, resume = asyncio.Event(), asyncio.Event()

        await _provisioner(mongo_client, name).provision(leaving)

        slow = _provisioner(
            cast(MongoClient, _StallsAfterTheMarkerWrite(mongo_client, wrote, resume)),
            name,
        )
        onboarding: asyncio.Task[None] | None = None

        try:
            async with _teardown_stalled_after_its_read(mongo_client, leaving, name):
                # Started inside the teardown's window, so its ownership read has already been
                # answered and cannot see what this onboarding is about to write.
                onboarding = asyncio.create_task(slow.provision(arriving))
                await asyncio.wait_for(wrote.wait(), timeout=10)

            # The teardown has now dropped the database and released its lock, and only now
            # does the onboarding get to look at either.
            with pytest.raises(CoreException) as caught:
                resume.set()
                await asyncio.wait_for(onboarding, timeout=10)

        finally:
            resume.set()

        assert caught.value.code == "tenant_onboarding_lost_its_database"
        assert caught.value.kind is ExceptionKind.CONCURRENCY
        assert name not in await _database_names(mongo_client)

    @pytest.mark.asyncio
    async def test_the_lock_is_read_before_the_marker_is_read_back(
        self,
        mongo_client: MongoClient,
        dropper: list[str],
    ) -> None:
        """The two reads are ordered, and the wrong order passes every other test here.

        Reading the marker first and the lock second, an onboarding interrupted between them
        for the length of a teardown sees its marker still present, then sees the lock already
        released, and calls that success — the exact failure both reads exist to prevent. The
        order that holds is lock first: whatever it saw there was true while the teardown was
        still holding it, so a later "no lock" cannot be mistaken for "never locked".

        Driven by stalling the onboarding after its first read, which is the only place the
        two orders behave differently at all.
        """

        leaving, arriving, name = _tenant(), _tenant(), _database(dropper, "readorder")
        read, resume = asyncio.Event(), asyncio.Event()

        await _provisioner(mongo_client, name).provision(leaving)

        interrupted = _provisioner(
            cast(MongoClient, _StallsAfterTheFirstRead(mongo_client, read, resume)),
            name,
        )
        onboarding: asyncio.Task[None] | None = None

        try:
            async with _teardown_stalled_after_its_read(mongo_client, leaving, name):
                onboarding = asyncio.create_task(interrupted.provision(arriving))
                await asyncio.wait_for(read.wait(), timeout=10)

            with pytest.raises(CoreException) as caught:
                resume.set()
                await asyncio.wait_for(onboarding, timeout=10)

        finally:
            resume.set()

        # The lock, read while the teardown still held it — not the marker, which was still
        # sitting there untouched at that moment and says nothing about what came next.
        assert caught.value.code == "tenant_offboarding_in_flight"
        assert name not in await _database_names(mongo_client)

    @pytest.mark.asyncio
    async def test_an_onboarding_looks_for_its_own_marker_not_any_marker(
        self,
        mongo_client: MongoClient,
        dropper: list[str],
    ) -> None:
        """ "Is the database still there" is the wrong question; "is *my* marker still there"
        is the right one, and under a colliding resolver they come apart.

        A teardown drops the database, a third tenant is onboarded into the same name and
        recreates it, and only then does the outrun onboarding look. A read that asked whether
        the collection held anything would find that newcomer's marker and report success for
        a tenant whose own marker went with the drop.
        """

        leaving, arriving, newcomer, name = (
            _tenant(),
            _tenant(),
            _tenant(),
            _database(dropper, "notmine"),
        )
        wrote, resume = asyncio.Event(), asyncio.Event()

        await _provisioner(mongo_client, name).provision(leaving)

        slow = _provisioner(
            cast(MongoClient, _StallsAfterTheMarkerWrite(mongo_client, wrote, resume)),
            name,
        )
        onboarding: asyncio.Task[None] | None = None

        try:
            async with _teardown_stalled_after_its_read(mongo_client, leaving, name):
                onboarding = asyncio.create_task(slow.provision(arriving))
                await asyncio.wait_for(wrote.wait(), timeout=10)

            # The database is gone and back again under somebody else's name.
            await _provisioner(mongo_client, name).provision(newcomer)

            with pytest.raises(CoreException) as caught:
                resume.set()
                await asyncio.wait_for(onboarding, timeout=10)

        finally:
            resume.set()

        assert caught.value.code == "tenant_onboarding_lost_its_database"
        assert caught.value.kind is ExceptionKind.CONCURRENCY
        assert [doc["_id"] for doc in await _markers(mongo_client, name)] == [
            str(newcomer.tenant_id)
        ]

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
        await mongo_client.insert_one(
            locks,
            {"_id": name, "tenant_id": "someone", "at": utcnow() - timedelta(minutes=5)},
        )

        # Read back rather than compared to what went in: a BSON date keeps milliseconds and
        # `utcnow()` has microseconds, so the value the server holds is the only one that can
        # be compared to itself.
        before = await _locks(mongo_client)

        with pytest.raises(CoreException) as caught:
            await provisioner.deprovision(tenant)

        assert caught.value.code == "tenant_offboarding_in_flight"
        assert caught.value.kind is ExceptionKind.CONCURRENCY
        assert name in await _database_names(mongo_client)

        # The refusal left the other holder's lock exactly as it found it: a teardown that
        # released a lock it never took would open the window for whoever does hold it.
        # Down to the stamp: how long the lock has been held is what an operator judges a
        # stuck one by, and a refusal that quietly restamped it would reset that clock every
        # time somebody retried the offboarding.
        assert await _locks(mongo_client) == before
        assert before[0]["tenant_id"] == "someone"

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
    async def test_every_read_in_the_protocol_goes_to_the_primary(
        self,
        mongo_container: MongoDbContainer,
        mongo_client: MongoClient,
        dropper: list[str],
    ) -> None:
        """Each read here is half of an ordering argument, and a secondary answers about a
        past the primary has left behind: a lock not yet replicated reads as no lock, a marker
        not yet replicated reads as dropped. An admin URI with `readPreference` set is all it
        takes, so the preference is pinned per read rather than inherited.
        """

        tenant, name = _tenant(), _database(dropper, "readpref")
        lagging = MongoClient()
        await lagging.initialize(
            f"{mongo_container.get_connection_url()}/?readPreference=secondaryPreferred",
            db_name=(await mongo_client.db()).name,
        )

        try:
            # The client really does default to something else — otherwise this asserts nothing.
            assert (await lagging.collection(_MARKER, db_name=name)).read_preference != (
                ReadPreference.PRIMARY
            )

            watched = _RecordsReadPreferences(lagging)
            provisioner = _provisioner(cast(MongoClient, watched), name, drop_on_deprovision=True)

            await provisioner.provision(tenant)
            await provisioner.deprovision(tenant)

            assert len(watched.preferences) == 3
            assert all(pref == ReadPreference.PRIMARY for pref in watched.preferences)

        finally:
            await lagging.close()

    @pytest.mark.asyncio
    async def test_the_metadata_read_behind_the_last_refusal_goes_to_the_primary_too(
        self,
        mongo_container: MongoDbContainer,
        mongo_client: MongoClient,
        dropper: list[str],
    ) -> None:
        """The refusal that protects a database this provisioner never registered rests on
        asking whether it holds anything, and that question is answered by collection metadata
        rather than by a document. A secondary that has not caught up reports a database full
        of somebody's data as absent, which is the single answer that lets the drop through.
        """

        tenant, name = _tenant(), _database(dropper, "metapref")
        lagging = MongoClient()
        await lagging.initialize(
            f"{mongo_container.get_connection_url()}/?readPreference=secondaryPreferred",
            db_name=(await mongo_client.db()).name,
        )

        try:
            coll = await mongo_client.collection("payroll", db_name=name)
            await mongo_client.insert_one(coll, {"amount": 1})

            watched = _RecordsReadPreferences(lagging)
            provisioner = _provisioner(cast(MongoClient, watched), name, drop_on_deprovision=True)

            with pytest.raises(CoreException) as caught:
                await provisioner.deprovision(tenant)

            assert caught.value.code == "tenant_database_not_provisioned"

            # Asserted on its own list rather than folded into the document reads: this is the
            # read the refusal above actually rests on, and a count over both would pass while
            # it alone went to a secondary.
            assert watched.metadata_preferences == [ReadPreference.PRIMARY]

        finally:
            await lagging.close()

    @pytest.mark.asyncio
    async def test_a_returning_holder_cannot_release_somebody_else_s_lock(
        self,
        mongo_client: MongoClient,
        dropper: list[str],
    ) -> None:
        """The recovery for a wedged name is a person deleting a row, which makes an unfenced
        release dangerous: a holder that was only slow — declared dead, cleared by hand,
        replaced — would come back and delete its successor's lock, opening the window under a
        teardown that did nothing wrong."""

        tenant, name = _tenant(), _database(dropper, "fenced")
        locks = await mongo_client.collection(_LOCK)

        await _provisioner(mongo_client, name).provision(tenant)

        async with _teardown_stalled_after_its_read(mongo_client, tenant, name):
            # Cleared by hand and retaken while this teardown is away.
            await mongo_client.delete_one(locks, {"_id": name})
            await mongo_client.insert_one(
                locks,
                {"_id": name, "tenant_id": "someone", "at": utcnow(), "owner": "the-successor"},
            )

        # The stalled teardown has now finished and released. The successor's lock stands.
        assert [doc["owner"] for doc in await _locks(mongo_client)] == ["the-successor"]

    @pytest.mark.asyncio
    async def test_a_drop_of_unknown_outcome_keeps_its_lock(
        self,
        mongo_client: MongoClient,
        dropper: list[str],
    ) -> None:
        """A `dropDatabase` that raises has not necessarily not happened — a timeout or a lost
        response leaves the server working while this side hears nothing. Releasing then would
        hand the name to an onboarding that writes into a database still being deleted, so the
        lock stays and a person decides. The teardown's own error is still what propagates."""

        tenant, name = _tenant(), _database(dropper, "unknown")

        await _provisioner(mongo_client, name).provision(tenant)

        broken = _provisioner(
            cast(MongoClient, _DropFails(mongo_client)),
            name,
            drop_on_deprovision=True,
        )

        with pytest.raises(RuntimeError, match="never came back"):
            await broken.deprovision(tenant)

        assert [doc["_id"] for doc in await _locks(mongo_client)] == [name]

        # And the name is held against both operations until somebody looks.
        for call in (broken.provision, broken.deprovision):
            with pytest.raises(CoreException) as caught:
                await call(tenant)

            assert caught.value.code == "tenant_offboarding_in_flight"

    @pytest.mark.asyncio
    async def test_a_teardown_that_refused_before_the_drop_releases_its_lock(
        self,
        mongo_client: MongoClient,
        dropper: list[str],
    ) -> None:
        """The other half of the rule above, and the half that matters more often: a refusal
        raised before the drop was issued knows the database is untouched, so keeping the lock
        would wedge the name on the misconfiguration path — every retry refused for a reason
        that has nothing to do with the one being reported."""

        first, second, name = _tenant(), _tenant(), _database(dropper, "refused")
        keeper = _provisioner(mongo_client, name)

        await keeper.provision(first)
        await keeper.provision(second)

        with pytest.raises(CoreException) as caught:
            await _provisioner(mongo_client, name, drop_on_deprovision=True).deprovision(first)

        assert caught.value.code == "tenant_database_shared_across_tenants"
        assert await _locks(mongo_client) == []

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
