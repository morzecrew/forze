"""`TenantManagementPort.list_tenants` — the global enumeration that drives per-tenant sweeps.

Until now the only way to ask "which tenants are there" was `list_principal_tenants`, which
answers a *different* question — which tenants may **this principal** see. Anything driven off
that visits only the tenants somebody happens to be a member of, so a sweep would quietly skip
a tenant with no members and report success.

The load-bearing test here is :meth:`TestInactiveTenants.test_a_deactivated_tenant_is_still_listed`.
Deactivating a tenant sets a flag; it does not delete a row, and it does not delete the
tenant's documents, blobs or counters. A sweep that filtered them out would drop real data and
call itself complete.
"""

from __future__ import annotations

from uuid import uuid4

from forze.application.contracts.tenancy import TenantIdentity
from forze_mock import MockState
from forze_mock.adapters.identity import MockTenantManagementPort

# ----------------------- #


def _port() -> MockTenantManagementPort:
    return MockTenantManagementPort(state=MockState(), route="tenancy")


async def _provision(port: MockTenantManagementPort, n: int) -> list[TenantIdentity]:
    return [await port.provision_tenant(tenant_key=f"t{i:02d}") for i in range(n)]


# ....................... #


class TestEnumeration:
    async def test_lists_every_tenant_with_the_total(self) -> None:
        port = _port()
        provisioned = await _provision(port, 5)

        tenants, total = await port.list_tenants()

        assert total == 5
        assert {t.tenant_id for t in tenants} == {t.tenant_id for t in provisioned}

    async def test_no_tenants_is_an_empty_page_not_an_error(self) -> None:
        tenants, total = await _port().list_tenants()

        assert list(tenants) == []
        assert total == 0

    async def test_pages_do_not_overlap_or_gap(self) -> None:
        port = _port()
        await _provision(port, 7)

        first, total = await port.list_tenants(limit=3, offset=0)
        second, _ = await port.list_tenants(limit=3, offset=3)
        third, _ = await port.list_tenants(limit=3, offset=6)

        ids = [t.tenant_id for t in (*first, *second, *third)]

        assert total == 7
        assert len(ids) == len(set(ids)) == 7  # every tenant exactly once
        assert ids == sorted(ids)  # …and in a stable order

    async def test_the_count_is_of_every_match_not_of_the_page(self) -> None:
        # A caller paging to exhaustion needs to know how far it has to go.
        port = _port()
        await _provision(port, 9)

        tenants, total = await port.list_tenants(limit=2)

        assert len(tenants) == 2
        assert total == 9

    async def test_a_tenant_with_no_members_is_still_listed(self) -> None:
        # The gap that made this port necessary: `list_principal_tenants` is membership-scoped,
        # so a tenant nobody belongs to is invisible to it — and to any sweep driven from it.
        port = _port()
        orphan = await port.provision_tenant(tenant_key="orphan")

        tenants, total = await port.list_tenants()

        assert total == 1
        assert [t.tenant_id for t in tenants] == [orphan.tenant_id]
        assert await port.list_principal_tenants(uuid4()) == []


# ....................... #


class TestInactiveTenants:
    async def test_a_deactivated_tenant_is_still_listed(self) -> None:
        # Deactivation is a flag, not a delete: the tenant's data is all still there. Skipping
        # it in a sweep would drop real records and report success — so the default answer is
        # the complete one.
        port = _port()
        live, gone = await _provision(port, 2)
        await port.deactivate_tenant(gone.tenant_id)

        tenants, total = await port.list_tenants()

        assert total == 2
        assert {t.tenant_id for t in tenants} == {live.tenant_id, gone.tenant_id}

    async def test_active_only_narrows_it_when_that_is_the_question(self) -> None:
        port = _port()
        live, gone = await _provision(port, 2)
        await port.deactivate_tenant(gone.tenant_id)

        tenants, total = await port.list_tenants(active_only=True)

        assert total == 1
        assert [t.tenant_id for t in tenants] == [live.tenant_id]

    async def test_active_only_is_opt_in_not_the_default(self) -> None:
        # Stated as its own test because the direction of the default is the whole safety
        # argument: missing a tenant is the dangerous failure, listing a dormant one is not.
        port = _port()
        (gone,) = await _provision(port, 1)
        await port.deactivate_tenant(gone.tenant_id)

        default, _ = await port.list_tenants()
        narrowed, _ = await port.list_tenants(active_only=True)

        assert len(default) == 1
        assert len(narrowed) == 0

    async def test_the_total_respects_the_filter(self) -> None:
        port = _port()
        tenants = await _provision(port, 4)

        for tenant in tenants[:3]:
            await port.deactivate_tenant(tenant.tenant_id)

        _, all_total = await port.list_tenants(limit=1)
        _, active_total = await port.list_tenants(limit=1, active_only=True)

        assert (all_total, active_total) == (4, 1)
