"""Unit tests for local_identity_deps helper."""

from __future__ import annotations

from uuid import UUID

import pytest

from forze.application.contracts.authn import AuthnDepKey
from forze.application.contracts.tenancy import TenantResolverDepKey
from forze_identity.builtin.local import LocalIdentityConfig, local_identity_deps
from forze_identity.builtin.local.config import LocalApiKeyEntry
from forze_identity.builtin.local.tenant_resolver import LocalTenantResolver

pytestmark = pytest.mark.unit

_PID = UUID("550e8400-e29b-41d4-a716-446655440000")
_TID = UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")


def test_local_identity_deps_registers_routes() -> None:
    config = LocalIdentityConfig.from_mapping(
        {"api_keys": {"k": {"principal_id": str(_PID), "tenant_id": str(_TID)}}},
    )

    deps = local_identity_deps(config)

    assert deps.exists(AuthnDepKey, route="main")
    assert deps.exists(TenantResolverDepKey, route="main")


async def test_a_directly_built_config_with_tenants_on_its_keys_wires_tenancy() -> None:
    # from_mapping folds key tenants into principal_tenants; a config built directly may
    # carry them on the keys alone, and they must count the same.
    config = LocalIdentityConfig(
        api_keys={"k": LocalApiKeyEntry(principal_id=_PID, tenant_id=_TID)},
    )

    deps = local_identity_deps(config)
    resolved = await LocalTenantResolver(config=config).resolve_from_principal(_PID)

    assert deps.exists(TenantResolverDepKey, route="main")
    assert resolved is not None
    assert resolved.tenant_id == _TID


def test_a_config_that_maps_no_tenant_wires_no_tenancy() -> None:
    # A resolver that can never bind a tenant would still declare a tenanted deployment,
    # whose realtime connections are refused without one.
    config = LocalIdentityConfig.from_mapping(
        {"api_keys": {"k": {"principal_id": str(_PID)}}},
    )

    deps = local_identity_deps(config)

    assert deps.exists(AuthnDepKey, route="main")
    assert not deps.exists(TenantResolverDepKey, route="main")
