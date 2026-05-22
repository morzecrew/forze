from typing import Callable

import attrs

from forze.application.contracts.authn import (
    AuthnIdentity,
    AuthnPort,
    IssuedTokens,
    PasswordCredentials,
    PasswordLifecyclePort,
    RefreshTokenCredentials,
    TokenLifecyclePort,
)
from forze.application.contracts.execution import Handler
from forze.base.errors import AuthenticationError

from ._utils import token_response_from_issued_tokens
from .dto import (
    AuthnChangePasswordRequestDTO,
    AuthnLoginRequestDTO,
    AuthnRefreshRequestDTO,
    AuthnTokenResponseDTO,
)

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class AuthnPasswordLogin(Handler[AuthnLoginRequestDTO, AuthnTokenResponseDTO]):
    """Handler for password-based authentication login."""

    authn: AuthnPort
    """Authentication port."""

    token_lifecycle: TokenLifecyclePort
    """Token lifecycle port."""

    # ....................... #

    async def __call__(self, args: AuthnLoginRequestDTO) -> AuthnTokenResponseDTO:
        creds = PasswordCredentials(
            login=args.login,
            password=args.password,
        )

        identity = await self.authn.authenticate_with_password(creds)
        tokens: IssuedTokens = await self.token_lifecycle.issue_tokens(identity)

        return token_response_from_issued_tokens(tokens)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class AuthnLogout(Handler[None, None]):
    """Usecase for revoking the current session(s) of an authenticated identity.

    Pulls the bound :class:`~forze.application.contracts.authn.AuthnIdentity`
    from the execution context and delegates to
    :meth:`~forze.application.contracts.authn.TokenLifecyclePort.revoke_tokens`.
    Raises :class:`AuthenticationError` when no identity is bound, so callers
    can surface a consistent 401 to clients.
    """

    resolver: Callable[[], AuthnIdentity | None]
    """Callable that resolves the current authenticated identity."""

    token_lifecycle: TokenLifecyclePort
    """Token lifecycle port."""

    # ....................... #

    async def __call__(self, args: None) -> None:
        _ = args

        identity = self.resolver()

        if identity is None:
            raise AuthenticationError(
                "Authentication required",
                code="auth_required",
            )

        await self.token_lifecycle.revoke_tokens(identity)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class AuthnRefreshTokens(Handler[AuthnRefreshRequestDTO, AuthnTokenResponseDTO]):
    """Handler for refreshing authentication tokens."""

    token_lifecycle: TokenLifecyclePort
    """Token lifecycle port."""

    # ....................... #

    async def __call__(self, args: AuthnRefreshRequestDTO) -> AuthnTokenResponseDTO:
        creds = RefreshTokenCredentials(token=args.refresh_token)

        tokens: IssuedTokens = await self.token_lifecycle.refresh_tokens(creds)

        return token_response_from_issued_tokens(tokens)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class AuthnChangePassword(Handler[AuthnChangePasswordRequestDTO, None]):
    """Handler for changing the password of the currently authenticated identity."""

    resolver: Callable[[], AuthnIdentity | None]
    """Callable that resolves the current authenticated identity."""

    password_lifecycle: PasswordLifecyclePort
    """Password lifecycle port."""

    # ....................... #

    async def __call__(self, args: AuthnChangePasswordRequestDTO) -> None:
        identity = self.resolver()

        if identity is None:
            raise AuthenticationError(
                "Authentication required",
                code="auth_required",
            )

        await self.password_lifecycle.change_password(identity, args.new_password)
