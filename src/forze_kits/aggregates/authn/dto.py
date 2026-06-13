from datetime import datetime
from uuid import UUID

from forze.domain.models import BaseDTO

# ----------------------- #


class AuthnLoginRequestDTO(BaseDTO):
    """DTO for authentication login request."""

    login: str
    """Login name (e.g. username or email address)."""

    password: str
    """Plaintext password."""


# ....................... #


class AuthnRefreshRequestDTO(BaseDTO):
    """DTO for authentication refresh request."""

    refresh_token: str
    """Refresh token."""


# ....................... #


class AuthnChangePasswordRequestDTO(BaseDTO):
    """DTO for authentication change-password request."""

    current_password: str
    """Current plaintext password; re-authenticated before the change is applied."""

    new_password: str
    """New plaintext password to set on the current identity's password account."""


# ....................... #


class AuthnRequestPasswordResetDTO(BaseDTO):
    """DTO for requesting a self-service password reset."""

    login: str
    """Login name the reset is requested for (e.g. username or email address)."""


# ....................... #


class AuthnPasswordResetAckDTO(BaseDTO):
    """Uniform acknowledgment for a password reset request (202-shaped).

    Returned for **every** request — known and unknown logins alike — so the
    response neither confirms nor denies that an account exists (no account
    enumeration). The reset token itself never appears here; it reaches the
    account holder out of band (see ``AuthnRequestPasswordReset``).
    """

    detail: str = "If an account exists for this login, a password reset has been initiated."
    """Static, login-independent acknowledgment text."""


# ....................... #


class AuthnResetPasswordDTO(BaseDTO):
    """DTO for confirming a self-service password reset."""

    token: str
    """Single-use reset token received out of band."""

    new_password: str
    """New plaintext password to set once the token verifies."""


# ....................... #


class AuthnTokenResponseDTO(BaseDTO):
    """DTO for authentication token response.

    Mirrors the OAuth2 token response shape; ``expires_in`` fields are seconds.
    Carriers (header/cookie) decide whether to leave token strings in the body
    or strip them (the body still describes scheme + lifetime so cookie clients
    know how long the credential is valid).
    """

    access_token: str | None = None
    """Access token; ``None`` when transported via cookie and stripped from body."""

    refresh_token: str | None = None
    """Refresh token; ``None`` when transported via cookie and stripped from body."""

    access_token_type: str = "Bearer"
    """Access token scheme label (e.g. ``Bearer``)."""

    access_expires_in: int | None = None
    """Lifetime of the access token in seconds, when known."""

    refresh_expires_in: int | None = None
    """Lifetime of the refresh token in seconds, when known."""


# ....................... #


class AuthnIssueApiKeyRequestDTO(BaseDTO):
    """DTO for a self-service API-key issuance request.

    The subject is the *current* authenticated identity (you mint keys for
    yourself); the request only carries optional metadata.
    """

    label: str | None = None
    """Optional human label for the key (e.g. "ChatGPT connection")."""

    actor_principal_id: UUID | None = None
    """Optional delegation agent: mint a user→agent key acting as this principal.
    The app decides which agents a caller may name; the engine still caps the key
    at the intersection of the user's and the agent's grants."""


# ....................... #


class AuthnIssuedApiKeyDTO(BaseDTO):
    """Response for a freshly issued API key — the **secret appears once**.

    Like the token response, this carries minted credential material in the body
    by design; it is the only time the raw key is returned. Persist it client-side
    now — subsequent listings expose only the non-secret ``hint``.
    """

    api_key: str
    """The raw API key. Shown once; never retrievable again."""

    key_id: str | None = None
    """Identifier of the key (used to revoke it)."""

    prefix: str | None = None
    """Presentation/routing prefix, when the key carries one."""

    hint: str | None = None
    """Non-secret fingerprint for later display."""

    label: str | None = None
    """The label echoed back (or ``None``)."""

    expires_at: datetime | None = None
    """Absolute expiry, or ``None`` for a non-expiring key."""


# ....................... #


class AuthnApiKeyListItemDTO(BaseDTO):
    """One non-secret key descriptor in a principal's key list."""

    key_id: UUID
    """Identifier of the key (used to revoke it)."""

    hint: str | None = None
    """Non-secret fingerprint of the key."""

    label: str | None = None
    """Optional human label; display ``label or hint``."""

    actor_principal_id: UUID | None = None
    """Delegation agent the key acts as, when it is a user→agent key."""

    prefix: str | None = None
    """Presentation/routing prefix, when the key carries one."""

    is_active: bool = True
    """Whether the key is still usable."""

    created_at: datetime | None = None
    """When the key was issued."""

    expires_at: datetime | None = None
    """Absolute expiry, or ``None`` for a non-expiring key."""


# ....................... #


class AuthnApiKeyListDTO(BaseDTO):
    """A principal's API keys (non-secret descriptors)."""

    keys: list[AuthnApiKeyListItemDTO] = []
    """The principal's keys, newest-first ordering left to the adapter."""


# ....................... #


class AuthnRevokeApiKeyRequestDTO(BaseDTO):
    """DTO for revoking one of the current identity's API keys."""

    id: UUID
    """Identifier of the key to revoke (``key_id`` from issuance/listing)."""
