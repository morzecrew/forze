from datetime import timedelta
from typing import Any, Mapping, Sequence

import attrs

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class AuthIdentity:
    """Basic auth identity representation."""

    subject_id: str
    """Subject identifier."""

    actor_id: str | None = attrs.field(default=None)
    """Actor identifier."""

    tenant_id: str | None = attrs.field(default=None)
    """Tenant identifier."""

    claims: Mapping[str, Any] | None = attrs.field(default=None)
    """Claims assigned to the identity."""

    roles: frozenset[str] = attrs.field(factory=frozenset)
    """Roles assigned to the identity."""

    permissions: frozenset[str] = attrs.field(factory=frozenset)
    """Permissions assigned to the identity."""

    is_active: bool = attrs.field(default=True)
    """Whether the identity is active."""


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class AuthorizationRequest:
    """Request for authorization."""

    action: str
    """Action to authorize, e.g. 'read', 'update', 'delete'."""

    resource: str | None = attrs.field(default=None)
    """Logical resource name, e.g. 'user', 'invoice', 'document'."""

    subject: Any | None = attrs.field(default=None)
    """Optional concrete domain object for attribute-based checks."""

    context: Mapping[str, Any] | None = attrs.field(default=None)
    """Additional authorization context."""


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class PasswordCredentials:
    """Credentials for password authentication."""

    login: str
    """Login name (e.g. username or email address)."""

    password: str
    """Plaintext or hashed password."""

    is_hashed: bool = attrs.field(default=False)
    """Whether the password is hashed."""


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

    expires_in: timedelta | None = attrs.field(default=None)
    """Time until the API key expires if applicable."""

    scopes: Sequence[str] | None = attrs.field(default=None)
    """Scope of the API key."""


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

    scopes: Sequence[str] | None = attrs.field(default=None)
    """Scope of the token."""


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class OAuth2Tokens:
    """Credentials for OAuth2 authentication."""

    access_token: TokenCredentials
    """Access token."""

    refresh_token: TokenCredentials | None = attrs.field(default=None)
    """Refresh token."""

    id_token: TokenCredentials | None = attrs.field(default=None)
    """ID token."""


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class OAuth2TokensResponse:
    """Response from OAuth2 token endpoint."""

    access_token: TokenResponse
    """Access token."""

    refresh_token: TokenResponse | None = attrs.field(default=None)
    """Refresh token."""

    id_token: TokenResponse | None = attrs.field(default=None)
    """ID token."""
