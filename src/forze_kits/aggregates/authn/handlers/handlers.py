from collections.abc import Callable, Sequence
from contextlib import nullcontext

import attrs

from forze.application.contracts.authn import (
    ApiKeyInfo,
    ApiKeyLifecyclePort,
    AuthnIdentity,
    AuthnPort,
    IssuedTokens,
    PasswordCredentials,
    PasswordLifecyclePort,
    RefreshTokenCredentials,
    RefreshTokenReuseError,
    TokenLifecyclePort,
)
from forze.application.contracts.execution import Handler
from forze.base.exceptions import CoreException, ExceptionKind, exc

from ..dto import (
    AuthnApiKeyListDTO,
    AuthnApiKeyListItemDTO,
    AuthnChangePasswordRequestDTO,
    AuthnIssueApiKeyRequestDTO,
    AuthnIssuedApiKeyDTO,
    AuthnLoginRequestDTO,
    AuthnPrincipalRefDTO,
    AuthnRefreshRequestDTO,
    AuthnRevokeApiKeyRequestDTO,
    AuthnTokenResponseDTO,
)
from ._utils import (
    TxScope,
    require_admin_identity,
    require_identity,
    require_own_identity,
    token_response_from_issued_tokens,
)

# ----------------------- #

_FAMILY_REVOCATION_ATTEMPTS = 3
"""Fresh transactions a reused token's family revocation gets before its conflict is raised."""

# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class AuthnPasswordLogin(Handler[AuthnLoginRequestDTO, AuthnTokenResponseDTO]):
    """Handler for password-based authentication login."""

    authn: AuthnPort
    """Authentication port."""

    token_lifecycle: TokenLifecyclePort
    """Token lifecycle port."""

    tx: TxScope = nullcontext
    """Opens the transaction the lifecycle calls run in."""

    # ....................... #

    async def __call__(self, args: AuthnLoginRequestDTO) -> AuthnTokenResponseDTO:
        creds = PasswordCredentials(
            login=args.login,
            password=args.password,
        )

        # The password check stays outside the transaction: it holds no lock, and hashing
        # inside one would hold a connection for the whole hash.
        authn = await self.authn.authenticate_with_password(creds)

        async with self.tx():
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

    tx: TxScope = nullcontext
    """Opens the transaction the lifecycle calls run in."""

    # ....................... #

    async def __call__(self, args: None) -> None:
        _ = args

        identity = require_own_identity(self.resolver)

        async with self.tx():
            await self.token_lifecycle.revoke_tokens(identity)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class AuthnRefreshTokens(Handler[AuthnRefreshRequestDTO, AuthnTokenResponseDTO]):
    """Handler for refreshing authentication tokens."""

    token_lifecycle: TokenLifecyclePort
    """Token lifecycle port."""

    tx: TxScope = nullcontext
    """Opens the transaction the lifecycle calls run in."""

    # ....................... #

    in_transaction: Callable[[], bool] | None = None
    """Whether a transaction is already open when the refresh starts."""

    # ....................... #

    async def __call__(self, args: AuthnRefreshRequestDTO) -> AuthnTokenResponseDTO:
        # Inside an enclosing transaction both scopes below are savepoints, and the re-raise
        # would roll the reuse revocation back with it: refuse rather than lose it quietly.
        if self.in_transaction is not None and self.in_transaction():
            raise exc.configuration(
                "refresh_tokens must not run inside an open transaction: a reused token's "
                "family is revoked in a transaction of its own, which an enclosing one would "
                "roll back. Run the operation without a plan-level transaction.",
                code="authn_refresh_in_transaction",
            )

        creds = RefreshTokenCredentials(token=args.refresh_token)

        try:
            async with self.tx():
                tokens: IssuedTokens = await self.token_lifecycle.refresh_tokens(creds)

        except RefreshTokenReuseError as reuse:
            # The raise rolled back the family revocation the lifecycle made before it, so
            # commit that revocation on its own: reuse is the token-theft signal.
            await self._revoke_family(reuse)
            raise

        return token_response_from_issued_tokens(tokens)

    # ....................... #

    async def _revoke_family(self, reuse: RefreshTokenReuseError) -> None:
        # A concurrent refresh can conflict with the revocation, and a backend may abort the
        # transaction on a conflict, so each attempt runs in a fresh one.
        for attempt in range(_FAMILY_REVOCATION_ATTEMPTS):
            try:
                async with self.tx():
                    await self.token_lifecycle.revoke_chain_of_tokens(
                        reuse.principal_id,
                        reuse.family_id,
                    )

                return

            except CoreException as error:
                last = attempt == _FAMILY_REVOCATION_ATTEMPTS - 1

                if error.kind is not ExceptionKind.CONCURRENCY or last:
                    raise


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class AuthnChangePassword(Handler[AuthnChangePasswordRequestDTO, None]):
    """Handler for changing the password of the currently authenticated identity."""

    resolver: Callable[[], AuthnIdentity | None]
    """Callable that resolves the current authenticated identity."""

    password_lifecycle: PasswordLifecyclePort
    """Password lifecycle port."""

    tx: TxScope = nullcontext
    """Opens the transaction the lifecycle calls run in."""

    # ....................... #

    async def __call__(self, args: AuthnChangePasswordRequestDTO) -> None:
        identity = require_own_identity(self.resolver)

        async with self.tx():
            await self.password_lifecycle.change_password(
                identity,
                args.current_password,
                args.new_password,
            )


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class AuthnIssueApiKey(Handler[AuthnIssueApiKeyRequestDTO, AuthnIssuedApiKeyDTO]):
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

    async def __call__(self, args: AuthnIssueApiKeyRequestDTO) -> AuthnIssuedApiKeyDTO:
        identity = require_own_identity(self.resolver)

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
            expires_at=(issued.lifetime.expires_at if issued.lifetime is not None else None),
        )


# ....................... #


def _key_list(keys: Sequence[ApiKeyInfo]) -> AuthnApiKeyListDTO:
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
class AuthnListPrincipalApiKeys(Handler[AuthnPrincipalRefDTO, AuthnApiKeyListDTO]):
    """Admin: list any principal's API keys (non-secret descriptors), to find one to revoke.

    Refuses a caller with no identity (401) or a delegated one (``delegate_denied``), as
    :class:`AuthnRevokePrincipalApiKey` does; who may administer is the guards' to decide.
    The listing is global: API-key accounts are not tenant-scoped.
    """

    resolver: Callable[[], AuthnIdentity | None]
    """Callable that resolves the current authenticated identity."""

    api_key_lifecycle: ApiKeyLifecyclePort
    """API key lifecycle port."""

    # ....................... #

    async def __call__(self, args: AuthnPrincipalRefDTO) -> AuthnApiKeyListDTO:
        require_admin_identity(self.resolver)

        return _key_list(await self.api_key_lifecycle.list_principal_api_keys(args.id))


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

        identity = require_identity(self.resolver)

        return _key_list(await self.api_key_lifecycle.list_api_keys(identity))


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
        identity = require_own_identity(self.resolver)

        await self.api_key_lifecycle.revoke_api_key(identity, str(args.id))


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class AuthnRevokePrincipalApiKey(Handler[AuthnRevokeApiKeyRequestDTO, None]):
    """Admin: revoke any principal's API key.

    Refuses a caller with no identity (401) or a delegated one (``delegate_denied``). Whether
    the caller may administer at all is the guards' to decide, which
    :func:`~forze_kits.aggregates.authn.build_authn_registry` requires before it registers
    this operation. The key is looked up by id alone, across tenants: API-key accounts are
    not tenant-scoped.
    """

    resolver: Callable[[], AuthnIdentity | None]
    """Callable that resolves the current authenticated identity."""

    api_key_lifecycle: ApiKeyLifecyclePort
    """API key lifecycle port."""

    # ....................... #

    async def __call__(self, args: AuthnRevokeApiKeyRequestDTO) -> None:
        require_admin_identity(self.resolver)

        await self.api_key_lifecycle.revoke_principal_api_key(str(args.id))
