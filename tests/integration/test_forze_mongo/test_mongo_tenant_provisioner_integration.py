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

import pytest
import pytest_asyncio

pytest.importorskip("pymongo")
pytest.importorskip("testcontainers.mongodb")

from testcontainers.mongodb import MongoDbContainer

from forze.application.contracts.tenancy import TenantIdentity
from forze.base.exceptions import CoreException
from forze_mongo.adapters.tenant_provisioner import MongoDatabaseTenantProvisioner
from forze_mongo.kernel.client.client import MongoClient
from forze_mongo.kernel.uri import with_mongo_credentials

# ----------------------- #

pytestmark = pytest.mark.integration

_MARKER = "_forze_tenants"


def _tenant() -> TenantIdentity:
    return TenantIdentity(tenant_id=uuid.uuid4())


def _database_for(prefix: str) -> str:
    """A per-test database-name resolver, namespaced so parallel tests cannot collide."""

    return f"forze_tp_{prefix}_{uuid.uuid4().hex[:8]}"


@pytest_asyncio.fixture
async def dropper(mongo_client: MongoClient) -> AsyncIterator[list[str]]:
    """Databases to remove afterwards — these tests create real ones outside the fixture's."""

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

        tenant, name = _tenant(), _database_for("exists")
        dropper.append(name)

        assert name not in await _database_names(mongo_client)

        await MongoDatabaseTenantProvisioner(
            client=mongo_client,
            database=lambda _: name,
        ).provision(tenant)

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

        tenant, name = _tenant(), _database_for("rerun")
        dropper.append(name)

        provisioner = MongoDatabaseTenantProvisioner(client=mongo_client, database=lambda _: name)

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
        """An upsert is check-then-act inside the server: onboardings that all find nothing
        all insert, and every loser gets a duplicate key on ``_id``. A retried onboarding, or
        two workers handed the same event, is that ordering — so the duplicate has to be the
        no-op it describes rather than a failed onboarding."""

        tenant, name = _tenant(), _database_for("race")
        dropper.append(name)

        provisioner = MongoDatabaseTenantProvisioner(client=mongo_client, database=lambda _: name)

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

        first, second, name = _tenant(), _tenant(), _database_for("shared")
        dropper.append(name)

        provisioner = MongoDatabaseTenantProvisioner(client=mongo_client, database=lambda _: name)

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

        name = _database_for("denied")
        dropper.append(name)

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
                    await MongoDatabaseTenantProvisioner(
                        client=confined,
                        database=lambda _: name,
                    ).provision(_tenant())

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
        tenant, name = _tenant(), _database_for("drop")
        dropper.append(name)

        provisioner = MongoDatabaseTenantProvisioner(
            client=mongo_client,
            database=lambda _: name,
            drop_on_deprovision=True,
        )

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

        tenant, name = _tenant(), _database_for("keep")
        dropper.append(name)

        provisioner = MongoDatabaseTenantProvisioner(client=mongo_client, database=lambda _: name)

        await provisioner.provision(tenant)
        await provisioner.deprovision(tenant)

        assert name in await _database_names(mongo_client)

    @pytest.mark.asyncio
    async def test_offboarding_an_absent_database_is_quiet(
        self,
        mongo_client: MongoClient,
    ) -> None:
        """Teardown is re-run at least as often as onboarding — a half-finished cleanup job, a
        retried command, an operator repeating themselves. The second pass has nothing to drop
        and nothing to complain about."""

        tenant, name = _tenant(), _database_for("absent")

        provisioner = MongoDatabaseTenantProvisioner(
            client=mongo_client,
            database=lambda _: name,
            drop_on_deprovision=True,
        )

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

        first, second, name = _tenant(), _tenant(), _database_for("cotenant")
        dropper.append(name)

        provisioner = MongoDatabaseTenantProvisioner(
            client=mongo_client,
            database=lambda _: name,
            drop_on_deprovision=True,
        )

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

        tenant, name = _tenant(), _database_for("foreign")
        dropper.append(name)

        coll = await mongo_client.collection("payroll", db_name=name)
        await mongo_client.insert_one(coll, {"amount": 1})

        provisioner = MongoDatabaseTenantProvisioner(
            client=mongo_client,
            database=lambda _: name,
            drop_on_deprovision=True,
        )

        with pytest.raises(CoreException) as caught:
            await provisioner.deprovision(tenant)

        assert caught.value.code == "tenant_database_not_provisioned"
        assert await mongo_client.count(coll, {}) == 1

    @pytest.mark.asyncio
    async def test_the_refused_database_can_be_offboarded_after_provisioning(
        self,
        mongo_client: MongoClient,
        dropper: list[str],
    ) -> None:
        """The recovery the refusal names has to actually work, or the refusal is a dead end
        for a database that really was the tenant's — provisioned by hand before this class
        was wired in."""

        tenant, name = _tenant(), _database_for("adopt")
        dropper.append(name)

        coll = await mongo_client.collection("payroll", db_name=name)
        await mongo_client.insert_one(coll, {"amount": 1})

        provisioner = MongoDatabaseTenantProvisioner(
            client=mongo_client,
            database=lambda _: name,
            drop_on_deprovision=True,
        )

        await provisioner.provision(tenant)
        await provisioner.deprovision(tenant)

        assert name not in await _database_names(mongo_client)
