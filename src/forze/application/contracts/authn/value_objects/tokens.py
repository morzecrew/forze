from uuid import UUID

import attrs

from .credentials import (
    AccessTokenCredentials,
    ApiKeyCredentials,
    RefreshTokenCredentials,
)
from .lifetime import CredentialLifetime

# ----------------------- #
# API key


@attrs.define(slots=True, kw_only=True, frozen=True)
class IssuedApiKey:
    """Response from API key endpoint."""

    key: ApiKeyCredentials
    """API key."""

    key_id: str | None = attrs.field(default=None)
    """Identifier of the issued API key, when the provider exposes one."""

    lifetime: CredentialLifetime | None = attrs.field(default=None)
    """Lifetime of the API key."""


# ....................... #
# Password invite


@attrs.define(slots=True, kw_only=True, frozen=True)
class IssuedInvite:
    """A single-use invite token freshly issued for a known principal.

    The raw ``token`` is delivered to the invitee out of band and presented back
    via ``accept_invite_with_password``; only its digest is persisted.
    """

    token: str
    """Opaque invite token string (only shown at issuance time)."""

    principal_id: UUID
    """Principal the invite provisions an account for once accepted."""

    lifetime: CredentialLifetime | None = attrs.field(default=None)
    """Lifetime of the invite."""


# ....................... #
# Access token


@attrs.define(slots=True, kw_only=True, frozen=True)
class IssuedAccessToken:
    """An access token freshly issued by a token lifecycle service."""

    token: AccessTokenCredentials
    """Access token credentials."""

    lifetime: CredentialLifetime | None = attrs.field(default=None)
    """Lifetime of the access token."""


# ....................... #
# Refresh token


@attrs.define(slots=True, kw_only=True, frozen=True)
class IssuedRefreshToken:
    """A refresh token freshly issued by a token lifecycle service."""

    token: RefreshTokenCredentials
    """Refresh token credentials."""

    lifetime: CredentialLifetime | None = attrs.field(default=None)
    """Lifetime of the refresh token."""


# ....................... #
# Token bundle


@attrs.define(slots=True, kw_only=True, frozen=True)
class IssuedTokens:
    """Bundle of tokens returned from issue/refresh flows.

    ``access`` is always present; ``refresh`` is omitted when the underlying
    lifecycle does not rotate refresh tokens (for example pure access-only
    flows or stateless verifier-only routes).
    """

    access: IssuedAccessToken
    """Issued access token."""

    refresh: IssuedRefreshToken | None = attrs.field(default=None)
    """Issued refresh token, when applicable."""
