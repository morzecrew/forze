"""Unit tests for :mod:`forze_tenancy` document resolver."""

from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import pytest

from forze.application.contracts.base import CountlessPage
from forze.base.primitives import utcnow
from forze_tenancy.adapters.resolver import TenantResolverAdapter
from forze_tenancy.application.specs import principal_tenant_binding_spec, tenant_spec
from forze_tenancy.domain.models.principal_tenant_binding import (
    ReadPrincipalTenantBinding,
)
from forze_tenancy.domain.models.tenant import ReadTenant


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
async def test_resolver_returns_none_when_inactive_and_verifying() -> None:
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

    got = await resolver.resolve_from_principal(pid)

    assert got is None
