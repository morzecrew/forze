from collections.abc import Awaitable, Sequence
from typing import Protocol, final
from uuid import UUID

import attrs

from forze.base.exceptions import CoreException, ExceptionKind

from ..value_objects import (
    ApiKeyCredentials,
    ApiKeyInfo,
    AuthnIdentity,
    IssuedApiKey,
    IssuedTokens,
    RefreshTokenCredentials,
)

# ----------------------- #


class PasswordLifecyclePort(Protocol):  # pragma: no cover
    """Manage the lifecycle of password accounts for an authenticated subject."""

    def change_password(
        self,
        identity: AuthnIdentity,
        current_password: str,
        new_password: str,
    ) -> Awaitable[None]:
        """Change the password after re-authenticating with the current password."""
        ...


# ....................... #


@final
@attrs.define(slots=True, eq=False)
class RefreshTokenReuseError(CoreException):
    """An already-rotated refresh token was presented: the token-theft signal.

    It renders as the uniform ``Invalid refresh token`` authentication error, so a caller
    cannot tell reuse from any other refused refresh. The principal and the family are
    attributes, never details, so they are not rendered either.

    :meth:`TokenLifecyclePort.refresh_tokens` revokes the family before raising this. Inside a
    transaction the raise rolls that revocation back, so a caller running refresh in one
    catches this once the transaction has ended and revokes the family again in its own.
    """

    principal_id: UUID = attrs.field(kw_only=True)
    """Principal the reused token belongs to."""

    family_id: UUID = attrs.field(kw_only=True)
    """Rotation family to revoke."""

    # ....................... #

    @classmethod
    def detected(cls, principal_id: UUID, family_id: UUID) -> "RefreshTokenReuseError":
        """Build the error for a reused token of ``family_id``."""

        return cls(
            kind=ExceptionKind.AUTHENTICATION,
            summary="Invalid refresh token",
            code=f"core.{ExceptionKind.AUTHENTICATION.value}",
            principal_id=principal_id,
            family_id=family_id,
        )


# ....................... #


class TokenLifecyclePort(Protocol):  # pragma: no cover
    """Issue, refresh, and revoke OAuth2-style token pairs for an authenticated subject.

    ``issue_tokens`` refuses a delegated identity (``delegate_denied``): a token carries the
    principal alone, so one minted for an agent acting for a user would drop the agent.

    ``issue_tokens`` and ``refresh_tokens`` check the principal's eligibility with a locking
    read, so run them in a transaction: a deactivation then either waits for the new session
    and revokes it, or commits first and the issue is refused. ``refresh_tokens`` raises
    :class:`RefreshTokenReuseError` for a reused token, after revoking its family.
    """

    def issue_tokens(
        self,
        identity: AuthnIdentity,
        *,
        tenant_id: UUID | None = None,
    ) -> Awaitable[IssuedTokens]: ...

    def refresh_tokens(
        self,
        refresh_token: RefreshTokenCredentials,
    ) -> Awaitable[IssuedTokens]: ...

    def revoke_tokens(
        self,
        identity: AuthnIdentity,
    ) -> Awaitable[None]: ...

    def revoke_chain_of_tokens(
        self,
        principal_id: UUID,
        family_id: UUID,
    ) -> Awaitable[None]:
        """Revoke every live session of one rotation family."""
        ...


# ....................... #


class ApiKeyLifecyclePort(Protocol):  # pragma: no cover
    """Manage the lifecycle of API keys for an authenticated subject."""

    def issue_api_key(
        self,
        identity: AuthnIdentity,
        *,
        actor_principal_id: UUID | None = None,
        label: str | None = None,
    ) -> Awaitable[IssuedApiKey]: ...

    def list_api_keys(
        self,
        identity: AuthnIdentity,
    ) -> Awaitable[Sequence[ApiKeyInfo]]: ...

    def refresh_api_key(
        self,
        credentials: ApiKeyCredentials,
    ) -> Awaitable[IssuedApiKey]: ...

    def revoke_api_key(
        self,
        identity: AuthnIdentity,
        key_id: str,
    ) -> Awaitable[None]: ...

    def revoke_many_api_keys(
        self,
        identity: AuthnIdentity,
        key_ids: Sequence[str],
    ) -> Awaitable[None]: ...

    def list_principal_api_keys(self, principal_id: UUID) -> Awaitable[Sequence[ApiKeyInfo]]:
        """List any principal's API keys, revoked ones included, as an administrator.

        Non-secret descriptors, as :meth:`list_api_keys` returns. No identity is checked
        here, as for :meth:`revoke_principal_api_key`, and the lookup is global: API-key
        accounts are not tenant-scoped. A principal with no keys, or none at all, lists
        empty.
        """
        ...

    def revoke_principal_api_key(self, key_id: str) -> Awaitable[None]:
        """Revoke any principal's API key, as an administrator.

        No identity is checked here: who may revoke another principal's key is the
        calling operation's authorization to settle. The key is found by id alone, in every
        tenant, since API-key accounts are not tenant-scoped. An unknown key is
        ``not_found``; a revoked one is left as it is and answers as a revocation does.
        """
        ...
