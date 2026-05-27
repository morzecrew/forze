"""Unit tests for local_identity_deps helper."""

from __future__ import annotations

from uuid import UUID

import pytest

from forze.application.contracts.authn import AuthnDepKey
from forze.application.contracts.tenancy import TenantResolverDepKey
from forze_identity.local import LocalIdentityConfig, local_identity_deps

pytestmark = pytest.mark.unit

_PID = UUID("550e8400-e29b-41d4-a716-446655440000")


def test_local_identity_deps_registers_routes() -> None:
    config = LocalIdentityConfig.from_mapping(
        {"api_keys": {"k": {"principal_id": str(_PID)}}},
    )

    deps = local_identity_deps(config)

    assert deps.exists(AuthnDepKey, route="main")
    assert deps.exists(TenantResolverDepKey, route="main")
