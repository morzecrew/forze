"""Tests for authn usecase handlers."""

from __future__ import annotations

from datetime import timedelta
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from forze.application.contracts.authn import (
    AuthnResult,
    IssuedAccessToken,
    IssuedRefreshToken,
    IssuedTokens,
)
from forze.application.contracts.authn.value_objects import (
    AccessTokenCredentials,
    AuthnIdentity,
    CredentialLifetime,
    RefreshTokenCredentials,
)
from forze.application.handlers.authn.dto import (
    AuthnChangePasswordRequestDTO,
    AuthnLoginRequestDTO,
    AuthnRefreshRequestDTO,
)
from forze.application.handlers.authn.handlers import (
    AuthnChangePassword,
    AuthnLogout,
    AuthnPasswordLogin,
    AuthnRefreshTokens,
)
from forze.base.exceptions import CoreException


def _issued_tokens() -> IssuedTokens:
    return IssuedTokens(
        access=IssuedAccessToken(
            token=AccessTokenCredentials(token="access"),
            lifetime=CredentialLifetime(expires_in=timedelta(seconds=30)),
        ),
        refresh=IssuedRefreshToken(
            token=RefreshTokenCredentials(token="refresh"),
            lifetime=CredentialLifetime(expires_in=timedelta(seconds=60)),
        ),
    )


class TestAuthnPasswordLogin:
    @pytest.mark.asyncio
    async def test_issues_tokens_on_success(self) -> None:
        principal_id = uuid4()
        authn = AsyncMock()
        authn.authenticate_with_password = AsyncMock(
            return_value=AuthnResult(identity=AuthnIdentity(principal_id=principal_id)),
        )
        token_lifecycle = AsyncMock()
        token_lifecycle.issue_tokens = AsyncMock(return_value=_issued_tokens())
        handler = AuthnPasswordLogin(authn=authn, token_lifecycle=token_lifecycle)

        dto = await handler(AuthnLoginRequestDTO(login="bob", password="pw"))

        authn.authenticate_with_password.assert_awaited_once()
        token_lifecycle.issue_tokens.assert_awaited_once()
        assert dto.access_token == "access"
        assert dto.refresh_token == "refresh"
        assert dto.access_expires_in == 30


class TestAuthnRefreshTokens:
    @pytest.mark.asyncio
    async def test_refresh_returns_token_dto(self) -> None:
        token_lifecycle = AsyncMock()
        token_lifecycle.refresh_tokens = AsyncMock(return_value=_issued_tokens())
        handler = AuthnRefreshTokens(token_lifecycle=token_lifecycle)

        dto = await handler(AuthnRefreshRequestDTO(refresh_token="rt"))

        token_lifecycle.refresh_tokens.assert_awaited_once()
        assert dto.access_token == "access"


class TestAuthnLogout:
    @pytest.mark.asyncio
    async def test_revokes_when_identity_present(self) -> None:
        identity = AuthnIdentity(principal_id=uuid4())
        token_lifecycle = AsyncMock()
        token_lifecycle.revoke_tokens = AsyncMock(return_value=None)
        handler = AuthnLogout(
            resolver=lambda: identity,
            token_lifecycle=token_lifecycle,
        )

        await handler(None)

        token_lifecycle.revoke_tokens.assert_awaited_once_with(identity)

    @pytest.mark.asyncio
    async def test_raises_when_identity_missing(self) -> None:
        handler = AuthnLogout(
            resolver=lambda: None,
            token_lifecycle=AsyncMock(),
        )

        with pytest.raises(CoreException, match="Authentication required"):
            await handler(None)


class TestAuthnChangePassword:
    @pytest.mark.asyncio
    async def test_changes_password_for_identity(self) -> None:
        identity = AuthnIdentity(principal_id=uuid4())
        password_lifecycle = AsyncMock()
        password_lifecycle.change_password = AsyncMock(return_value=None)
        handler = AuthnChangePassword(
            resolver=lambda: identity,
            password_lifecycle=password_lifecycle,
        )

        await handler(AuthnChangePasswordRequestDTO(new_password="new"))

        password_lifecycle.change_password.assert_awaited_once_with(identity, "new")

    @pytest.mark.asyncio
    async def test_raises_when_identity_missing(self) -> None:
        handler = AuthnChangePassword(
            resolver=lambda: None,
            password_lifecycle=AsyncMock(),
        )

        with pytest.raises(CoreException, match="Authentication required"):
            await handler(AuthnChangePasswordRequestDTO(new_password="x"))
