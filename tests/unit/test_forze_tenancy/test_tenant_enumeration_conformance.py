"""The real `TenantManagementAdapter.list_tenants` ≡ the mock's, over real document ports.

The sibling test in this directory drives the real adapter with ``MagicMock`` document ports,
which cannot tell whether a filter expression is even well-formed or a sort key exists — an
``AsyncMock`` returns a page for any arguments at all. So this one wires the real adapter over
the **mock document adapter**: a real query implementation, with the real filter DSL, the real
sort resolution and the real page/count semantics, just in memory.

Driving both implementations off the same provisioning script and comparing them is what makes
the in-memory ``MockTenantManagementPort`` trustworthy as the oracle every other tenancy test
leans on — otherwise a mock-backed proof about tenant enumeration is a proof about the mock.
"""

from __future__ import annotations

from typing import Any

import pytest

from forze.application.contracts.tenancy import TenantIdentity
from forze_identity.tenancy.adapters.management import TenantManagementAdapter
from forze_identity.tenancy.application.specs import (
    principal_tenant_binding_spec,
    tenant_spec,
)
from forze_mock import MockDepsModule, MockState
from forze_mock.adapters.identity import MockTenantManagementPort
from tests.support.execution_context import context_from_deps

# ----------------------- #


def _real_adapter() -> TenantManagementAdapter:
    """The document-backed adapter, over the mock document plane (real query semantics)."""

    ctx = context_from_deps(MockDepsModule(state=MockState())())

    return TenantManagementAdapter(
        tenant_qry=ctx.doc.query(tenant_spec),
        tenant_cmd=ctx.doc.command(tenant_spec),
        binding_qry=ctx.doc.query(principal_tenant_binding_spec),
        binding_cmd=ctx.doc.command(principal_tenant_binding_spec),
    )


def _mock_port() -> MockTenantManagementPort:
    return MockTenantManagementPort(state=MockState(), route="tenancy")


async def _script(port: Any) -> tuple[list[TenantIdentity], TenantIdentity]:
    """Provision five tenants and deactivate one — the same story on both ports."""

    tenants = [await port.provision_tenant(tenant_key=f"t{i:02d}") for i in range(5)]
    await port.deactivate_tenant(tenants[2].tenant_id)

    return tenants, tenants[2]


async def _snapshot(port: Any) -> dict[str, Any]:
    """Everything an enumerating caller can observe."""

    everyone, everyone_total = await port.list_tenants()
    active, active_total = await port.list_tenants(active_only=True)
    first, first_total = await port.list_tenants(limit=2, offset=0)
    second, _ = await port.list_tenants(limit=2, offset=2)

    return {
        "all_keys": sorted(t.tenant_key for t in everyone),
        "all_total": everyone_total,
        "active_keys": sorted(t.tenant_key for t in active),
        "active_total": active_total,
        "page_one": [t.tenant_key for t in first],
        "page_two": [t.tenant_key for t in second],
        "page_total": first_total,
        "pages_disjoint": not ({t.tenant_id for t in first} & {t.tenant_id for t in second}),
    }


# ....................... #


@pytest.mark.asyncio
async def test_real_adapter_matches_the_mock_port() -> None:
    real = _real_adapter()
    mock = _mock_port()

    await _script(real)
    await _script(mock)

    real_snap = await _snapshot(real)
    mock_snap = await _snapshot(mock)

    assert real_snap == mock_snap

    # …and they agree on the right answer, not merely on the same wrong one. The deactivated
    # tenant is in the complete list — its rows still exist, so a sweep must still visit it.
    assert real_snap["all_total"] == 5
    assert real_snap["active_total"] == 4
    assert len(real_snap["all_keys"]) == 5
    assert real_snap["pages_disjoint"]


@pytest.mark.asyncio
async def test_the_filter_and_sort_are_ones_the_document_plane_accepts() -> None:
    # The point of using a real query port: a malformed filter expression or a sort on a
    # field that does not exist raises here, where MagicMock would have returned a page.
    real = _real_adapter()
    _, deactivated = await _script(real)

    active, total = await real.list_tenants(active_only=True, limit=10)

    assert total == 4
    assert deactivated.tenant_id not in {t.tenant_id for t in active}


@pytest.mark.asyncio
async def test_paging_walks_every_tenant_exactly_once() -> None:
    real = _real_adapter()
    await _script(real)

    seen: list[TenantIdentity] = []
    offset = 0

    while True:
        page, total = await real.list_tenants(limit=2, offset=offset)

        if not page:
            break

        seen.extend(page)
        offset += len(page)

    assert len(seen) == total == 5
    assert len({t.tenant_id for t in seen}) == 5  # no repeats across page boundaries
