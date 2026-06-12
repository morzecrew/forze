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
