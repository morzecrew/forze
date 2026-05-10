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
# Abstract token credentials


@attrs.define(slots=True, kw_only=True, frozen=True)
class TokenCredentials:
    """Credentials for token authentication.

    ``scheme`` and ``kind`` are routing hints, not security gates; verifier implementations
    decide whether to consult them. ``profile`` selects an explicit verifier registration
    when more than one verifier is wired for the same authn route (overrides the spec hint
    when set).
    """

    token: str
    """Opaque access or identity token."""

    scheme: str | None = attrs.field(default=None)
    """Optional token scheme hint, e.g. ``"Bearer"``."""

    kind: str | None = attrs.field(default=None)
    """Optional token kind hint, e.g. ``"access"``, ``"refresh"``, ``"id"``."""

    profile: str | None = attrs.field(default=None)
    """Optional verifier profile name; overrides ``AuthnSpec.token_profile`` when set."""
