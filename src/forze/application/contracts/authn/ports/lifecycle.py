from collections.abc import Awaitable, Sequence
from typing import Protocol
from uuid import UUID

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


class TokenLifecyclePort(Protocol):  # pragma: no cover
    """Issue, refresh, and revoke OAuth2-style token pairs for an authenticated subject.

    ``issue_tokens`` refuses a delegated identity (``delegate_denied``): a token carries the
    principal alone, so one minted for an agent acting for a user would drop the agent.
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

    def revoke_principal_api_key(self, key_id: str) -> Awaitable[None]:
        """Revoke any principal's API key, as an administrator.

        No identity is checked here: who may revoke another principal's key is the
        calling operation's authorization to settle. The key is found by id alone, in every
        tenant, since API-key accounts are not tenant-scoped. An unknown key is
        ``not_found``; a revoked one is left as it is and answers as a revocation does.
        """
        ...
