"""Unit tests for :class:`PrincipalDeactivationAdapter`."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from forze.application.contracts.authn import AuthnIdentity
from forze_identity.authn.adapters.principal_deactivation import PrincipalDeactivationAdapter

pytestmark = pytest.mark.unit


@pytest.mark.asyncio
async def test_deactivate_cascades_registry_tokens_and_credentials() -> None:
    pid = uuid4()
    registry = MagicMock()
    registry.deactivate_principal = AsyncMock()
    token_lifecycle = MagicMock()
    token_lifecycle.revoke_tokens = AsyncMock()
    credentials = MagicMock()
    credentials.deactivate_all = AsyncMock()

    adapter = PrincipalDeactivationAdapter(
        principal_registry=registry,
        token_lifecycle=token_lifecycle,
        credentials=credentials,
    )

    await adapter.deactivate(pid)

    registry.deactivate_principal.assert_awaited_once_with(pid)
    token_lifecycle.revoke_tokens.assert_awaited_once_with(
        AuthnIdentity(principal_id=pid),
    )
    credentials.deactivate_all.assert_awaited_once_with(pid)
