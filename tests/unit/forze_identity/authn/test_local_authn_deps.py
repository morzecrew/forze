"""Unit tests for local identity authn dependency wiring."""

from __future__ import annotations

from uuid import UUID

import pytest

from forze.application.contracts.authn import AuthnDepKey
from forze_identity.authn import AuthnDepsModule, AuthnKernelConfig
from forze_identity.authn.execution.deps import ConfigurableLocalApiKeyVerifier
from forze_identity.local import LocalIdentityConfig

pytestmark = pytest.mark.unit

_PID = UUID("550e8400-e29b-41d4-a716-446655440000")


def test_authn_deps_module_without_api_key_pepper() -> None:
    config = LocalIdentityConfig.from_mapping(
        {"api_keys": {"k": {"principal_id": str(_PID)}}},
    )

    deps = AuthnDepsModule(
        kernel=AuthnKernelConfig(),
        authn={"main": frozenset({"api_key"})},
        api_key_verifiers={
            "main": ConfigurableLocalApiKeyVerifier(config=config),
        },
    )()

    assert deps.exists(AuthnDepKey, route="main")
