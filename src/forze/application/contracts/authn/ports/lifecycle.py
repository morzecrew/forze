from typing import Awaitable, Protocol, Sequence
from uuid import UUID

from ..value_objects import (
    ApiKeyCredentials,
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
        identity: AuthnIdentity,  # noqa: F841
        current_password: str,  # noqa: F841
        new_password: str,  # noqa: F841
    ) -> Awaitable[None]:
        """Change the password after re-authenticating with the current password."""
        ...


# ....................... #


class TokenLifecyclePort(Protocol):  # pragma: no cover
    """Issue, refresh, and revoke OAuth2-style token pairs for an authenticated subject."""

    def issue_tokens(
        self,
        identity: AuthnIdentity,  # noqa: F841
        *,
        tenant_id: UUID | None = None,
    ) -> Awaitable[IssuedTokens]: ...

    def refresh_tokens(
        self,
        refresh_token: RefreshTokenCredentials,  # noqa: F841
    ) -> Awaitable[IssuedTokens]: ...

    def revoke_tokens(
        self,
        identity: AuthnIdentity,  # noqa: F841
    ) -> Awaitable[None]: ...


# ....................... #


class ApiKeyLifecyclePort(Protocol):  # pragma: no cover
    """Manage the lifecycle of API keys for an authenticated subject."""

    def issue_api_key(
        self,
        identity: AuthnIdentity,  # noqa: F841
        *,
        actor_principal_id: UUID | None = None,  # noqa: F841
    ) -> Awaitable[IssuedApiKey]: ...

    def refresh_api_key(
        self,
        credentials: ApiKeyCredentials,  # noqa: F841
    ) -> Awaitable[IssuedApiKey]: ...

    def revoke_api_key(
        self,
        identity: AuthnIdentity,  # noqa: F841
        key_id: str,  # noqa: F841
    ) -> Awaitable[None]: ...

    def revoke_many_api_keys(
        self,
        identity: AuthnIdentity,  # noqa: F841
        key_ids: Sequence[str],  # noqa: F841
    ) -> Awaitable[None]: ...
