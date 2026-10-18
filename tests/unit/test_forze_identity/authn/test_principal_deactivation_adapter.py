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


class _Revocation:
    def require_transaction(self) -> None:
        pass


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("revocation", "order"),
    [
        # Without revocation states nothing raises an epoch here: the order is unchanged.
        (None, ["principal", "credentials", "sessions"]),
        # With them, a password change takes the account before the epoch the principal's
        # write may raise, so the credentials go first.
        (_Revocation(), ["credentials", "principal", "sessions"]),
    ],
)
async def test_deactivate_orders_its_writes_by_whether_epochs_are_raised(
    revocation: object,
    order: list[str],
) -> None:
    calls: list[str] = []
    registry = MagicMock()
    registry.deactivate_principal = AsyncMock(side_effect=lambda _pid: calls.append("principal"))
    token_lifecycle = MagicMock()
    token_lifecycle.revoke_tokens = AsyncMock(side_effect=lambda _i: calls.append("sessions"))
    credentials = MagicMock()
    credentials.deactivate_all = AsyncMock(side_effect=lambda _pid: calls.append("credentials"))

    adapter = PrincipalDeactivationAdapter(
        principal_registry=registry,
        token_lifecycle=token_lifecycle,
        credentials=credentials,
        revocation=revocation,  # type: ignore[arg-type]
    )

    await adapter.deactivate(uuid4())

    assert calls == order
