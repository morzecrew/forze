"""Unit tests for LocalTenantResolver."""

from __future__ import annotations

from uuid import UUID, uuid4

import pytest

from forze.base.exceptions import CoreException
from forze_identity.builtin.local import LocalIdentityConfig, LocalTenantResolver

pytestmark = pytest.mark.unit

_PID = UUID("550e8400-e29b-41d4-a716-446655440000")
_TID = UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")


@pytest.mark.asyncio
async def test_resolve_from_principal_map() -> None:
    config = LocalIdentityConfig.from_mapping(
        {
            "api_keys": {},
            "principal_tenants": {str(_PID): str(_TID)},
        },
    )
    resolver = LocalTenantResolver(config=config)

    identity = await resolver.resolve_from_principal(_PID)

    assert identity is not None
    assert identity.tenant_id == _TID


@pytest.mark.asyncio
async def test_default_tenant_fallback() -> None:
    pid = uuid4()
    config = LocalIdentityConfig.from_mapping(
        {
            "api_keys": {},
            "default_tenant_id": str(_TID),
        },
    )
    resolver = LocalTenantResolver(config=config)

    identity = await resolver.resolve_from_principal(pid)

    assert identity is not None
    assert identity.tenant_id == _TID


@pytest.mark.asyncio
async def test_requested_tenant_mismatch() -> None:
    config = LocalIdentityConfig.from_mapping(
        {
            "api_keys": {},
            "principal_tenants": {str(_PID): str(_TID)},
        },
    )
    resolver = LocalTenantResolver(config=config)

    with pytest.raises(CoreException, match="tenant"):
        await resolver.resolve_from_principal(_PID, requested_tenant_id=uuid4())
