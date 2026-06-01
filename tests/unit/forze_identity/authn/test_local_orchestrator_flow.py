"""Smoke test: orchestrator + local API key verifier + JwtNativeUuidResolver."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest

from forze.application.contracts.authn import ApiKeyCredentials, AuthnSpec
from forze_identity.authn import AuthnOrchestrator, JwtNativeUuidResolver
from forze_identity.builtin.local import LocalApiKeyVerifier, LocalIdentityConfig

pytestmark = pytest.mark.unit

_PID = UUID("550e8400-e29b-41d4-a716-446655440000")


@pytest.mark.asyncio
async def test_orchestrator_local_api_key_round_trip() -> None:
    config = LocalIdentityConfig.from_mapping(
        {"api_keys": {"dev": {"principal_id": str(_PID)}}},
    )
    eligibility = MagicMock()
    eligibility.require_authentication_allowed = AsyncMock()
    orchestrator = AuthnOrchestrator(
        resolver=JwtNativeUuidResolver(),
        eligibility=eligibility,
        enabled_methods=frozenset({"api_key"}),
        api_key_verifier=LocalApiKeyVerifier(config=config),
    )
    spec = AuthnSpec(name="main", enabled_methods=("api_key",))

    result = await orchestrator.authenticate_with_api_key(
        ApiKeyCredentials(key="dev"),
    )

    assert result.identity.principal_id == _PID
