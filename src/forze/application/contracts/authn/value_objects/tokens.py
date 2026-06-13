from datetime import datetime
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

    hint: str | None = attrs.field(default=None)
    """Non-secret fingerprint of the key (e.g. ``ab12…wxyz``) for display."""

    label: str | None = attrs.field(default=None)
    """Optional human label for the key (the issued one echoes the request)."""

    lifetime: CredentialLifetime | None = attrs.field(default=None)
    """Lifetime of the API key."""


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class ApiKeyInfo:
    """Non-secret descriptor of an issued API key, for listing/management.

    Carries everything a "connected apps" UI needs to show and revoke a key —
    **never** the secret or its hash. ``hint`` is a stable fingerprint and ``label``
    an optional human name; show ``label or hint``.
    """

    key_id: UUID
    """Identifier of the key (used to revoke it)."""

    hint: str | None = attrs.field(default=None)
    """Non-secret fingerprint of the key for display."""

    label: str | None = attrs.field(default=None)
    """Optional human label."""

    actor_principal_id: UUID | None = attrs.field(default=None)
    """Delegation agent the key acts as, when it is a user→agent key."""

    prefix: str | None = attrs.field(default=None)
    """Presentation/routing prefix, when the key carries one."""

    is_active: bool = attrs.field(default=True)
    """Whether the key is still usable (revocation flips this off)."""

    created_at: datetime | None = attrs.field(default=None)
    """When the key was issued."""

    expires_at: datetime | None = attrs.field(default=None)
    """Absolute expiry, or ``None`` for a non-expiring key."""


# ....................... #
# Password invite


@attrs.define(slots=True, kw_only=True, frozen=True)
class IssuedInvite:
    """A single-use invite token freshly issued for a known principal.

    The raw ``token`` is delivered to the invitee out of band and presented back
    via ``accept_invite_with_password``; only its digest is persisted.
    """

    token: str = attrs.field(repr=False)
    """Opaque invite token string (only shown at issuance time)."""

    principal_id: UUID
    """Principal the invite provisions an account for once accepted."""

    lifetime: CredentialLifetime | None = attrs.field(default=None)
    """Lifetime of the invite."""


# ....................... #
# Password reset


@attrs.define(slots=True, kw_only=True, frozen=True)
class IssuedPasswordReset:
    """A single-use password reset token freshly issued for a known login.

    The raw ``token`` is delivered to the account holder out of band (e-mail,
    SMS, …) and presented back via ``reset_password``; only its digest is
    persisted. It must never appear in the response of the operation that
    requested it (no-enumeration posture).
    """

    token: str = attrs.field(repr=False)
    """Opaque reset token string (only shown at issuance time)."""

    principal_id: UUID
    """Principal whose password the reset re-keys once confirmed."""

    login: str
    """Login the reset was requested for (delivery-channel lookup key)."""

    expires_at: datetime
    """Absolute expiration time of the reset token."""


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
