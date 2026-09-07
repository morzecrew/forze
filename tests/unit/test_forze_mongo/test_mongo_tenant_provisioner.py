"""Mongo database tenant provisioner: the wirings it refuses and the names it resolves.

Everything that depends on what the server actually does — an upsert racing itself, a
``dropDatabase``, whether an unwritten database reports collections — lives in the integration
suite against a real ``mongod``. What is here is what is decided *before* any of that: the two
constructions that cannot be made safe, and the resolved-name checks that stand between a
resolver's return value and a destructive command.

# covers: MongoDatabaseTenantProvisioner
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import pytest

from forze.application.contracts.tenancy import TenantIdentity
from forze.base.exceptions import CoreException
from forze_mongo import MongoDatabaseTenantProvisioner, RoutedMongoClient

# ----------------------- #

_TENANT = TenantIdentity(tenant_id=uuid4())


def _provisioner(**overrides: Any) -> MongoDatabaseTenantProvisioner:
    options: dict[str, Any] = {
        "client": AsyncMock(name="client"),
        "database": lambda tid: f"tenant_{tid}",
    }
    options.update(overrides)

    return MongoDatabaseTenantProvisioner(**options)


# ....................... #


class TestRefusedWiring:
    def test_a_routed_client_is_refused(self) -> None:
        """A routed client picks its connection from the ambient tenant, and the tenant being
        onboarded is not the ambient one — so every onboarding would land somewhere else, or
        nowhere, and nothing downstream could tell."""

        routed = RoutedMongoClient(
            secrets=MagicMock(name="secrets"),
            secret_ref_for_tenant={},
            tenant_provider=lambda: None,
            database_name_for_tenant=lambda tid: str(tid),
        )

        with pytest.raises(CoreException, match="routed Mongo client"):
            _provisioner(client=routed)

    def test_a_static_database_may_not_be_dropped(self) -> None:
        """One name for every tenant means offboarding the first destroys the rest. The name
        on its own is allowed — a shared database with per-tenant collections is real — so it
        is the pairing that is refused, at wiring, before a tenant exists to lose."""

        with pytest.raises(CoreException, match="destroy the data of all of them"):
            _provisioner(database="shared_app", drop_on_deprovision=True)

        # Either half alone builds.
        assert _provisioner(database="shared_app").drop_on_deprovision is False
        assert _provisioner(drop_on_deprovision=True).drop_on_deprovision is True

    def test_teardown_is_off_by_default(self) -> None:
        """`dropDatabase` destroys the tenant, so it is never what an unconfigured
        provisioner does."""

        assert _provisioner().drop_on_deprovision is False


class TestResolvedName:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("system", ["admin", "config", "local"])
    async def test_a_system_database_is_refused(self, system: str) -> None:
        """A resolver returns a string and ``"admin"`` is a string. Dropping it takes the
        deployment's users, replica-set config and oplog with it."""

        provisioner = _provisioner(database=lambda _: system, drop_on_deprovision=True)

        for call in (provisioner.provision, provisioner.deprovision):
            with pytest.raises(CoreException, match="system database"):
                await call(_TENANT)

    @pytest.mark.asyncio
    async def test_an_empty_name_is_refused(self) -> None:
        """The driver would read an empty database name as "the client's default", which is
        the admin connection's own database — a resolver returning nothing must not silently
        provision there."""

        provisioner = _provisioner(database=lambda _: "")

        with pytest.raises(CoreException, match="empty database name"):
            await provisioner.provision(_TENANT)

    @pytest.mark.asyncio
    async def test_the_passed_tenant_names_the_database_not_the_ambient_one(self) -> None:
        """The provisioner's contract: an admin onboards tenant X while not acting as X, so
        the resolver must be fed the argument rather than anything ambient."""

        seen: list[UUID | None] = []
        client = AsyncMock(name="client")
        client.find_one.return_value = None  # no offboarding in flight

        async def _resolve(tid: UUID | None) -> str:
            seen.append(tid)
            return f"tenant_{tid}"

        await _provisioner(client=client, database=_resolve).provision(_TENANT)

        assert seen == [_TENANT.tenant_id]

        # The first collection resolved is the marker's, in the tenant's own database; the
        # second is the offboarding lock's, which is deliberately somewhere else.
        marker_call = client.collection.await_args_list[0]
        assert marker_call.kwargs["db_name"] == f"tenant_{_TENANT.tenant_id}"

    @pytest.mark.asyncio
    async def test_teardown_off_resolves_nothing_at_all(self) -> None:
        """Not merely "skips the drop": a deprovision that resolved names and read the marker
        collection would still fail an offboarding on a resolver that had since broken, for a
        teardown nobody asked for."""

        client = AsyncMock(name="client")

        await _provisioner(client=client, database=lambda _: "admin").deprovision(_TENANT)

        client.collection.assert_not_called()
        client.db.assert_not_called()


class TestLockLocation:
    @pytest.mark.asyncio
    async def test_a_client_with_no_database_of_its_own_is_named(self) -> None:
        """The lock defaults to the client's own database, so a client without one has to say
        which knob fixes it — the driver's "database name is not configured" names neither the
        lock nor ``lock_database``, and it would surface from an onboarding that has nothing
        obviously to do with either."""

        client = AsyncMock(name="client")
        client.db.side_effect = CoreException.configuration("Mongo database name is not configured")

        with pytest.raises(CoreException, match="lock_database"):
            await _provisioner(client=client).provision(_TENANT)

    @pytest.mark.asyncio
    async def test_the_lock_is_read_from_the_configured_database(self) -> None:
        """Not the tenant's: a collection inside the database a teardown drops goes with the
        drop, so the lock has to be resolved somewhere else even on the onboarding path."""

        client = AsyncMock(name="client")
        client.find_one.return_value = None

        await _provisioner(client=client, lock_database="ops").provision(_TENANT)

        marker, lock = client.collection.await_args_list

        assert marker.kwargs["db_name"] == f"tenant_{_TENANT.tenant_id}"
        assert lock.args[0] == "_forze_tenant_offboarding"
        assert lock.kwargs["db_name"] == "ops"
