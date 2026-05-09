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


class AuthnTokenResponseDTO(BaseDTO):
    """DTO for authentication token response."""

    access_token: str
    """Access token."""

    refresh_token: str | None = None
    """Refresh token."""

    access_token_type: str = "bearer"
    """Token type (e.g. Bearer)."""
