from typing import Callable

import attrs

from forze.application.contracts.authn import (
    ApiKeyLifecyclePort,
    AuthnIdentity,
    AuthnPort,
    IssuedTokens,
    PasswordCredentials,
    PasswordLifecyclePort,
    RefreshTokenCredentials,
    TokenLifecyclePort,
)
from forze.application.contracts.execution import Handler
from forze.base.exceptions import exc

from ..dto import (
    AuthnApiKeyListDTO,
    AuthnApiKeyListItemDTO,
    AuthnChangePasswordRequestDTO,
    AuthnIssueApiKeyRequestDTO,
    AuthnIssuedApiKeyDTO,
    AuthnLoginRequestDTO,
    AuthnRefreshRequestDTO,
    AuthnRevokeApiKeyRequestDTO,
    AuthnTokenResponseDTO,
)
from ._utils import token_response_from_issued_tokens


def _require_identity(
    resolver: "Callable[[], AuthnIdentity | None]",
) -> AuthnIdentity:
    """Pull the bound identity or raise the uniform 401 (self-service guard)."""

    identity = resolver()

    if identity is None:
        raise exc.authentication("Authentication required", code="auth_required")

    return identity

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

        authn = await self.authn.authenticate_with_password(creds)
        tokens: IssuedTokens = await self.token_lifecycle.issue_tokens(authn.identity)

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
            raise exc.authentication(
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
            raise exc.authentication(
                "Authentication required",
                code="auth_required",
            )

        await self.password_lifecycle.change_password(
            identity,
            args.current_password,
            args.new_password,
        )


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class AuthnIssueApiKey(
    Handler[AuthnIssueApiKeyRequestDTO, AuthnIssuedApiKeyDTO]
):
    """Self-service: issue an API key for the current identity (secret returned once).

    Optionally a user→agent **delegation** key (``actor_principal_id``). The secret
    is in the response body by design — the only time it is returned — exactly like
    a token response.
    """

    resolver: Callable[[], AuthnIdentity | None]
    """Callable that resolves the current authenticated identity."""

    api_key_lifecycle: ApiKeyLifecyclePort
    """API key lifecycle port."""

    # ....................... #

    async def __call__(
        self, args: AuthnIssueApiKeyRequestDTO
    ) -> AuthnIssuedApiKeyDTO:
        identity = _require_identity(self.resolver)

        issued = await self.api_key_lifecycle.issue_api_key(
            identity,
            actor_principal_id=args.actor_principal_id,
            label=args.label,
        )

        return AuthnIssuedApiKeyDTO(
            api_key=issued.key.key,
            key_id=issued.key_id,
            prefix=issued.key.prefix,
            hint=issued.hint,
            label=issued.label,
            expires_at=(
                issued.lifetime.expires_at if issued.lifetime is not None else None
            ),
        )


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class AuthnListApiKeys(Handler[None, AuthnApiKeyListDTO]):
    """Self-service: list the current identity's API keys (non-secret descriptors)."""

    resolver: Callable[[], AuthnIdentity | None]
    """Callable that resolves the current authenticated identity."""

    api_key_lifecycle: ApiKeyLifecyclePort
    """API key lifecycle port."""

    # ....................... #

    async def __call__(self, args: None) -> AuthnApiKeyListDTO:
        _ = args

        identity = _require_identity(self.resolver)

        keys = await self.api_key_lifecycle.list_api_keys(identity)

        return AuthnApiKeyListDTO(
            keys=[
                AuthnApiKeyListItemDTO(
                    key_id=info.key_id,
                    hint=info.hint,
                    label=info.label,
                    actor_principal_id=info.actor_principal_id,
                    prefix=info.prefix,
                    is_active=info.is_active,
                    created_at=info.created_at,
                    expires_at=info.expires_at,
                )
                for info in keys
            ]
        )


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class AuthnRevokeApiKey(Handler[AuthnRevokeApiKeyRequestDTO, None]):
    """Self-service: revoke one of the current identity's API keys.

    The lifecycle port scopes revocation to the caller's own keys (a key id that
    is not theirs is rejected as not-found), so this cannot revoke another
    principal's key.
    """

    resolver: Callable[[], AuthnIdentity | None]
    """Callable that resolves the current authenticated identity."""

    api_key_lifecycle: ApiKeyLifecyclePort
    """API key lifecycle port."""

    # ....................... #

    async def __call__(self, args: AuthnRevokeApiKeyRequestDTO) -> None:
        identity = _require_identity(self.resolver)

        await self.api_key_lifecycle.revoke_api_key(identity, str(args.id))
