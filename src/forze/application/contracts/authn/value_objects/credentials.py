import attrs

# ----------------------- #
# Password


@attrs.define(slots=True, kw_only=True, frozen=True)
class PasswordCredentials:
    """Credentials for password authentication."""

    login: str
    """Login name (e.g. username or email address)."""

    password: str
    """Plaintext password."""


# ....................... #
# API key


@attrs.define(slots=True, kw_only=True, frozen=True)
class ApiKeyCredentials:
    """Credentials for API key authentication."""

    key: str
    """API key."""

    prefix: str | None = attrs.field(default=None)
    """Optional prefix for the API key."""


# ....................... #
# Access token (verifier-consumed bearer-style credentials)


@attrs.define(slots=True, kw_only=True, frozen=True)
class AccessTokenCredentials:
    """Credentials carrying an access token presented for verification.

    ``scheme`` is a routing/labeling hint (for OAuth2-style ``Bearer ...``
    headers); ``profile`` selects an explicit verifier registration when more
    than one token verifier is wired on the same authn route (overrides the
    spec hint when set).
    """

    token: str
    """Opaque access token string."""

    scheme: str = attrs.field(default="Bearer")
    """Token scheme label, e.g. ``"Bearer"``."""

    profile: str | None = attrs.field(default=None)
    """Optional verifier profile name; overrides ``AuthnSpec.token_profile`` when set."""


# ....................... #
# Refresh token (lifecycle-consumed)


@attrs.define(slots=True, kw_only=True, frozen=True)
class RefreshTokenCredentials:
    """Credentials carrying a refresh token presented for token rotation.

    Refresh tokens are validated by the lifecycle service (digest/expiry/chain),
    never by a token verifier port; therefore there is no ``profile`` knob.
    """

    token: str
    """Opaque refresh token string."""
