"""Unit tests for :mod:`forze_identity.tenancy` document resolver."""

from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import pytest

from forze.application.contracts.base import CountlessPage
from forze.application.contracts.cache import CacheSpec
from forze.application.contracts.document import DocumentSpec
from forze.base.exceptions import CoreException
from forze.base.primitives import utcnow
from forze.testing import context_from_modules
from forze_identity.tenancy.adapters.resolver import TenantResolverAdapter
from forze_identity.tenancy.application.specs import principal_tenant_binding_spec, tenant_spec
from forze_identity.tenancy.domain.models.principal_tenant_binding import (
    ReadPrincipalTenantBinding,
)
from forze_identity.tenancy.domain.models.tenant import ReadTenant
from forze_mock import MockDepsModule
from tests.support.tenant_memberships import management


def _binding_row(*, pid: UUID, tid: UUID) -> ReadPrincipalTenantBinding:
    now = utcnow()
    return ReadPrincipalTenantBinding(
        id=uuid4(),
        rev=1,
        created_at=now,
        last_update_at=now,
        principal_id=pid,
        tenant_id=tid,
    )

def test_resolver_post_init_rejects_cache_and_history() -> None:
    binding_qry = AsyncMock()
    binding_qry.spec = DocumentSpec(
        name=principal_tenant_binding_spec.name,
        read=ReadPrincipalTenantBinding,
        cache=CacheSpec(name="cache"),
    )

    with pytest.raises(CoreException, match="caching is forbidden"):
        TenantResolverAdapter(binding_qry=binding_qry, tenant_qry=None)

    binding_qry.spec = principal_tenant_binding_spec

    tenant_qry = AsyncMock()
    tenant_qry.spec = DocumentSpec(
        name=tenant_spec.name,
        read=ReadTenant,
        history_enabled=True,
    )

    with pytest.raises(CoreException, match="history is forbidden"):
        TenantResolverAdapter(binding_qry=binding_qry, tenant_qry=tenant_qry)

@pytest.mark.asyncio
async def test_resolver_returns_tenant_from_binding() -> None:
    pid = uuid4()
    tid = uuid4()
    bind = _binding_row(pid=pid, tid=tid)

    binding_qry = AsyncMock()
    binding_qry.spec = principal_tenant_binding_spec
    binding_qry.find_many = AsyncMock(
        return_value=CountlessPage(hits=[bind], page=1, size=1),
    )

    resolver = TenantResolverAdapter(binding_qry=binding_qry, tenant_qry=None)

    got = await resolver.resolve_from_principal(pid)

    assert got is not None
    assert got.tenant_id == tid

@pytest.mark.asyncio
async def test_resolver_raises_tenant_inactive_when_verifying() -> None:
    pid = uuid4()
    tid = uuid4()
    bind = _binding_row(pid=pid, tid=tid)

    binding_qry = AsyncMock()
    binding_qry.spec = principal_tenant_binding_spec
    binding_qry.find_many = AsyncMock(
        return_value=CountlessPage(hits=[bind], page=1, size=1),
    )

    now = utcnow()
    tenant_row = ReadTenant(
        id=tid,
        rev=1,
        created_at=now,
        last_update_at=now,
        tenant_key="acme",
        is_active=False,
    )

    tenant_qry = AsyncMock()
    tenant_qry.spec = tenant_spec
    tenant_qry.get = AsyncMock(return_value=tenant_row)

    resolver = TenantResolverAdapter(binding_qry=binding_qry, tenant_qry=tenant_qry)

    with pytest.raises(CoreException, match="inactive") as ei:
        await resolver.resolve_from_principal(pid)

    assert ei.value.code == "tenant_inactive"

@pytest.mark.asyncio
async def test_resolver_raises_tenant_mismatch_when_hint_not_in_membership() -> None:
    pid = uuid4()
    requested = uuid4()

    binding_qry = AsyncMock()
    binding_qry.spec = principal_tenant_binding_spec
    binding_qry.find_many = AsyncMock(
        return_value=CountlessPage(hits=[], page=1, size=0),
    )

    resolver = TenantResolverAdapter(binding_qry=binding_qry, tenant_qry=None)

    with pytest.raises(CoreException, match="Requested tenant") as ei:
        await resolver.resolve_from_principal(pid, requested_tenant_id=requested)

    assert ei.value.code == "tenant_mismatch"

@pytest.mark.asyncio
async def test_resolver_uses_requested_tenant_when_present() -> None:
    pid = uuid4()
    tid = uuid4()
    bind = _binding_row(pid=pid, tid=tid)

    binding_qry = AsyncMock()
    binding_qry.spec = principal_tenant_binding_spec
    binding_qry.find_many = AsyncMock(
        return_value=CountlessPage(hits=[bind], page=1, size=1),
    )

    resolver = TenantResolverAdapter(binding_qry=binding_qry, tenant_qry=None)

    got = await resolver.resolve_from_principal(pid, requested_tenant_id=tid)

    assert got is not None
    assert got.tenant_id == tid

@pytest.mark.asyncio
async def test_resolver_binds_no_tenant_when_principal_membership_is_ambiguous() -> None:
    # Several memberships and no requested tenant is the state right after a password
    # login: the principal must reach the selector, so no tenant is bound (#666).
    pid = uuid4()
    bind_a = _binding_row(pid=pid, tid=uuid4())
    bind_b = _binding_row(pid=pid, tid=uuid4())

    binding_qry = AsyncMock()
    binding_qry.spec = principal_tenant_binding_spec
    binding_qry.find_many = AsyncMock(
        return_value=CountlessPage(hits=[bind_a, bind_b], page=1, size=2),
    )

    resolver = TenantResolverAdapter(binding_qry=binding_qry, tenant_qry=None)

    assert await resolver.resolve_from_principal(pid) is None

@pytest.mark.asyncio
async def test_resolver_binds_no_tenant_without_membership() -> None:
    binding_qry = AsyncMock()
    binding_qry.spec = principal_tenant_binding_spec
    binding_qry.find_many = AsyncMock(
        return_value=CountlessPage(hits=[], page=1, size=0),
    )

    resolver = TenantResolverAdapter(binding_qry=binding_qry, tenant_qry=None)

    assert await resolver.resolve_from_principal(uuid4()) is None

@pytest.mark.asyncio
async def test_resolver_refuses_a_non_member_request_from_a_principal_in_several_tenants() -> None:
    # Over real (mock) binding documents: binding no tenant for an ambiguous principal must
    # not loosen what a requested tenant has to be.
    ctx = context_from_modules(MockDepsModule())
    mgmt = management(ctx)
    pid = uuid4()
    first = await mgmt.provision_tenant(tenant_key="acme")
    second = await mgmt.provision_tenant(tenant_key="globex")
    stranger = await mgmt.provision_tenant(tenant_key="initech")
    await mgmt.attach_principal(pid, first.tenant_id)
    await mgmt.attach_principal(pid, second.tenant_id)
    resolver = TenantResolverAdapter(
        binding_qry=ctx.doc.query(principal_tenant_binding_spec),
        tenant_qry=ctx.doc.query(tenant_spec),
    )

    assert await resolver.resolve_from_principal(pid) is None

    picked = await resolver.resolve_from_principal(pid, requested_tenant_id=second.tenant_id)
    assert picked is not None
    assert picked.tenant_id == second.tenant_id

    for requested in (stranger.tenant_id, uuid4()):
        with pytest.raises(CoreException) as ei:
            await resolver.resolve_from_principal(pid, requested_tenant_id=requested)

        assert ei.value.code == "tenant_mismatch"
