"""Tests for the self-service password reset handlers (request + confirm)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from forze.application.contracts.authn import IssuedPasswordReset
from forze.base.exceptions import CoreException, ExceptionKind, exc
from forze_kits.aggregates.authn import (
    AUTHN_PASSWORD_RESET_REQUESTED,
    AuthnPasswordResetAckDTO,
    AuthnRequestPasswordReset,
    AuthnRequestPasswordResetDTO,
    AuthnResetPassword,
    AuthnResetPasswordDTO,
)

# ----------------------- #


def _issued(login: str = "alice") -> IssuedPasswordReset:
    return IssuedPasswordReset(
        token="raw-reset-token",
        principal_id=uuid4(),
        login=login,
        expires_at=datetime.now(tz=UTC) + timedelta(hours=1),
    )


class TestAuthnRequestPasswordReset:
    @pytest.mark.asyncio
    async def test_known_and_unknown_logins_get_the_identical_ack(self) -> None:
        known_port = AsyncMock()
        known_port.request_reset = AsyncMock(return_value=_issued())
        unknown_port = AsyncMock()
        unknown_port.request_reset = AsyncMock(return_value=None)

        known_ack = await AuthnRequestPasswordReset(password_reset=known_port)(
            AuthnRequestPasswordResetDTO(login="alice"),
        )
        unknown_ack = await AuthnRequestPasswordReset(password_reset=unknown_port)(
            AuthnRequestPasswordResetDTO(login="nobody"),
        )

        # Byte-identical uniform ack — no account enumeration via the body.
        assert known_ack == unknown_ack
        assert isinstance(known_ack, AuthnPasswordResetAckDTO)
        unknown_port.request_reset.assert_awaited_once_with("nobody")

    @pytest.mark.asyncio
    async def test_ack_never_carries_the_token(self) -> None:
        issued = _issued()
        port = AsyncMock()
        port.request_reset = AsyncMock(return_value=issued)

        ack = await AuthnRequestPasswordReset(password_reset=port)(
            AuthnRequestPasswordResetDTO(login="alice"),
        )

        assert issued.token not in str(ack.model_dump())

    @pytest.mark.asyncio
    async def test_outbox_staging_carries_the_token_when_configured(self) -> None:
        issued = _issued()
        port = AsyncMock()
        port.request_reset = AsyncMock(return_value=issued)
        outbox = AsyncMock()

        handler = AuthnRequestPasswordReset(password_reset=port, outbox=outbox)
        await handler(AuthnRequestPasswordResetDTO(login="alice"))

        outbox.stage.assert_awaited_once()
        event_type, payload = outbox.stage.await_args.args
        assert event_type == AUTHN_PASSWORD_RESET_REQUESTED
        assert payload.token == issued.token
        assert payload.login == issued.login
        assert payload.principal_id == issued.principal_id
        assert payload.expires_at == issued.expires_at
        outbox.flush.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_unknown_login_stages_nothing(self) -> None:
        port = AsyncMock()
        port.request_reset = AsyncMock(return_value=None)
        outbox = AsyncMock()

        handler = AuthnRequestPasswordReset(password_reset=port, outbox=outbox)
        ack = await handler(AuthnRequestPasswordResetDTO(login="nobody"))

        assert isinstance(ack, AuthnPasswordResetAckDTO)
        outbox.stage.assert_not_called()
        outbox.flush.assert_not_called()

    @pytest.mark.asyncio
    async def test_without_outbox_the_ack_still_lands(self) -> None:
        port = AsyncMock()
        port.request_reset = AsyncMock(return_value=_issued())

        ack = await AuthnRequestPasswordReset(password_reset=port)(
            AuthnRequestPasswordResetDTO(login="alice"),
        )

        assert isinstance(ack, AuthnPasswordResetAckDTO)


class TestAuthnResetPassword:
    @pytest.mark.asyncio
    async def test_delegates_to_the_port(self) -> None:
        port = AsyncMock()
        port.reset_password = AsyncMock()

        await AuthnResetPassword(password_reset=port)(
            AuthnResetPasswordDTO(token="tok", new_password="pw-2"),
        )

        port.reset_password.assert_awaited_once_with("tok", "pw-2")

    @pytest.mark.asyncio
    async def test_uniform_port_error_propagates_as_authentication(self) -> None:
        port = AsyncMock()
        port.reset_password = AsyncMock(
            side_effect=exc.authentication("Invalid or expired reset token"),
        )

        with pytest.raises(CoreException, match="Invalid or expired reset token") as ei:
            await AuthnResetPassword(password_reset=port)(
                AuthnResetPasswordDTO(token="bad", new_password="pw-2"),
            )

        assert ei.value.kind is ExceptionKind.AUTHENTICATION
