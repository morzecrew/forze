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
    """DTO for authentication change-password request.

    Requires an authenticated identity at call time (resolved from the execution
    context). Re-authentication with the current password is intentionally not
    part of this DTO; callers wanting strong change-password guards can compose
    :class:`~forze.application.usecases.authn.AuthnPasswordLogin` first.
    """

    new_password: str
    """New plaintext password to set on the current identity's password account."""


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
