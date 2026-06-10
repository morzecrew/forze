"""Tests for :class:`~forze_identity.tenancy.adapters.management.TenantManagementAdapter`."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from forze.application.contracts.base import Page
from forze.application.contracts.cache import CacheSpec
from forze.application.contracts.document import DocumentSpec
from forze.base.exceptions import CoreException
from forze_identity.tenancy.adapters.management import TenantManagementAdapter
from forze_identity.tenancy.application.specs import (
    principal_tenant_binding_spec,
    tenant_spec,
)
from forze_identity.tenancy.domain.models.principal_tenant_binding import (
    ReadPrincipalTenantBinding,
)
from forze_identity.tenancy.domain.models.tenant import ReadTenant


def _adapter() -> TenantManagementAdapter:
    tenant_qry = MagicMock()
    tenant_qry.spec = tenant_spec
    tenant_qry.get = AsyncMock()

    tenant_cmd = MagicMock()
    tenant_cmd.spec = tenant_spec
    tenant_cmd.create = AsyncMock()
    tenant_cmd.update = AsyncMock()

    binding_qry = MagicMock()
    binding_qry.spec = principal_tenant_binding_spec
    binding_qry.find_many = AsyncMock(
        return_value=Page(hits=[], count=0, page=1, size=10),
    )

    binding_cmd = MagicMock()
    binding_cmd.spec = principal_tenant_binding_spec
    binding_cmd.create = AsyncMock()
    binding_cmd.kill = AsyncMock()

    return TenantManagementAdapter(
        tenant_qry=tenant_qry,
        tenant_cmd=tenant_cmd,
        binding_qry=binding_qry,
        binding_cmd=binding_cmd,
    )


def test_post_init_rejects_mismatched_specs() -> None:
    adapter = _adapter()
    adapter.tenant_qry.spec = DocumentSpec(name="wrong", read=ReadTenant)
    with pytest.raises(CoreException, match="tenant_qry spec"):
        adapter.__attrs_post_init__()


def test_post_init_rejects_cache_and_history() -> None:
    adapter = _adapter()

    adapter.tenant_qry.spec = DocumentSpec(
        name=tenant_spec.name,
        read=ReadTenant,
        cache=CacheSpec(name="cache"),
    )
    with pytest.raises(CoreException, match="caching is forbidden"):
        adapter.__attrs_post_init__()

    adapter.tenant_qry.spec = tenant_spec
    adapter.binding_cmd.spec = DocumentSpec(
        name=principal_tenant_binding_spec.name,
        read=ReadPrincipalTenantBinding,
        history_enabled=True,
    )
    with pytest.raises(CoreException, match="history is forbidden"):
        adapter.__attrs_post_init__()


@pytest.mark.asyncio
async def test_provision_tenant() -> None:
    adapter = _adapter()
    tid = uuid4()
    now = datetime.now(tz=timezone.utc)
    row = ReadTenant(
        id=tid,
        rev=1,
        created_at=now,
        last_update_at=now,
        tenant_key="acme",
        is_active=True,
    )
    adapter.tenant_cmd.create = AsyncMock(return_value=row)
    identity = await adapter.provision_tenant(tenant_key="acme")
    assert identity.tenant_id == tid
    assert identity.tenant_key == "acme"


@pytest.mark.asyncio
async def test_attach_principal_skips_duplicate() -> None:
    adapter = _adapter()
    pid, tid = uuid4(), uuid4()
    adapter.binding_qry.find_many = AsyncMock(
        return_value=Page(hits=[MagicMock()], count=1, page=1, size=1),
    )
    await adapter.attach_principal(pid, tid)
    adapter.binding_cmd.create.assert_not_awaited()


@pytest.mark.asyncio
async def test_attach_principal_creates_binding() -> None:
    adapter = _adapter()
    await adapter.attach_principal(uuid4(), uuid4())
    adapter.binding_cmd.create.assert_awaited_once()


@pytest.mark.asyncio
async def test_detach_principal_kills_bindings() -> None:
    adapter = _adapter()
    hit = MagicMock()
    hit.id = uuid4()
    adapter.binding_qry.find_many = AsyncMock(
        return_value=Page(hits=[hit], count=1, page=1, size=10),
    )
    await adapter.detach_principal(uuid4(), uuid4())
    adapter.binding_cmd.kill.assert_awaited_once_with(hit.id)


@pytest.mark.asyncio
async def test_deactivate_tenant() -> None:
    adapter = _adapter()
    tid = uuid4()
    now = datetime.now(tz=timezone.utc)
    row = ReadTenant(
        id=tid,
        rev=1,
        created_at=now,
        last_update_at=now,
        tenant_key="x",
        is_active=True,
    )
    adapter.tenant_qry.get = AsyncMock(return_value=row)
    await adapter.deactivate_tenant(tid)
    adapter.tenant_cmd.update.assert_awaited_once()
