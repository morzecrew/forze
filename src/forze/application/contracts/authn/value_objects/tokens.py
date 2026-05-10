import attrs

from .credentials import ApiKeyCredentials, TokenCredentials
from .lifetime import CredentialLifetime

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class ApiKeyResponse:
    """Response from API key endpoint."""

    key: ApiKeyCredentials
    """API key."""

    key_id: str | None = attrs.field(default=None)
    """Identifier of the issued API key, when the provider exposes one."""

    lifetime: CredentialLifetime | None = attrs.field(default=None)
    """Lifetime of the API key."""


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class TokenResponse:
    """Response from token endpoint."""

    token: TokenCredentials
    """Token."""

    lifetime: CredentialLifetime | None = attrs.field(default=None)
    """Lifetime of the token."""


# ....................... #
# OAuth2 tokens


@attrs.define(slots=True, kw_only=True, frozen=True)
class OAuth2Tokens:
    """Credentials for OAuth2 authentication.

    ``access_token`` is optional so refresh-only flows do not need a placeholder access
    token to satisfy the shape.
    """

    access_token: TokenCredentials | None = attrs.field(default=None)
    """Access token (optional; omit for refresh-only requests)."""

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
