from datetime import datetime, timedelta
from uuid import UUID

import attrs

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class AuthnIdentity:
    """Authentication subject representation."""

    principal_id: UUID
    """Principal ID."""


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class PasswordCredentials:
    """Credentials for password authentication."""

    login: str
    """Login name (e.g. username or email address)."""

    password: str
    """Plaintext password."""


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class ApiKeyCredentials:
    """Credentials for API key authentication."""

    key: str
    """API key."""

    prefix: str | None = attrs.field(default=None)
    """Optional prefix for the API key."""


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class ApiKeyResponse:
    """Response from API key endpoint."""

    key: ApiKeyCredentials
    """API key."""

    key_id: str | None = attrs.field(default=None)
    """Identifier of the issued API key, when the provider exposes one."""

    expires_in: timedelta | None = attrs.field(default=None)
    """Time until the API key expires if applicable."""

    expires_at: datetime | None = attrs.field(default=None)
    """Absolute expiration time if known."""


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class TokenCredentials:
    """Credentials for token authentication."""

    token: str
    """Opaque access or identity token."""

    scheme: str | None = attrs.field(default=None)
    """Optional token scheme, e.g. Bearer."""

    kind: str | None = attrs.field(default=None)
    """Optional token kind, e.g. access, refresh, id."""


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class TokenResponse:
    """Response from token endpoint."""

    token: TokenCredentials
    """Token."""

    expires_in: timedelta | None = attrs.field(default=None)
    """Time until the token expires if applicable."""

    issued_at: datetime | None = attrs.field(default=None)
    """Absolute issue time if known."""

    expires_at: datetime | None = attrs.field(default=None)
    """Absolute expiration time if known."""


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class OAuth2Tokens:
    """Credentials for OAuth2 authentication."""

    access_token: TokenCredentials
    """Access token."""

    refresh_token: TokenCredentials | None = attrs.field(default=None)
    """Refresh token."""


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class OAuth2TokensResponse:
    """Response from OAuth2 token endpoint."""

    access_token: TokenResponse
    """Access token."""

    refresh_token: TokenResponse | None = attrs.field(default=None)
    """Refresh token."""
