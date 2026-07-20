"""Unit tests for :mod:`forze_identity.tenancy` document resolver."""

from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import pytest

from forze.application.contracts.base import CountlessPage
from forze.application.contracts.cache import CacheSpec
from forze.application.contracts.document import DocumentSpec
from forze.base.exceptions import CoreException
from forze.base.primitives import utcnow
from forze_identity.tenancy.adapters.resolver import TenantResolverAdapter
from forze_identity.tenancy.application.specs import principal_tenant_binding_spec, tenant_spec
from forze_identity.tenancy.domain.models.principal_tenant_binding import (
    ReadPrincipalTenantBinding,
)
from forze_identity.tenancy.domain.models.tenant import ReadTenant


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
async def test_resolver_raises_when_principal_membership_is_ambiguous() -> None:
    pid = uuid4()
    bind_a = _binding_row(pid=pid, tid=uuid4())
    bind_b = _binding_row(pid=pid, tid=uuid4())

    binding_qry = AsyncMock()
    binding_qry.spec = principal_tenant_binding_spec
    binding_qry.find_many = AsyncMock(
        return_value=CountlessPage(hits=[bind_a, bind_b], page=1, size=2),
    )

    resolver = TenantResolverAdapter(binding_qry=binding_qry, tenant_qry=None)

    with pytest.raises(CoreException, match="ambiguous"):
        await resolver.resolve_from_principal(pid)
