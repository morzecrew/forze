"""Auth requirement value object for HTTP transport policies."""

from typing import final

import attrs

from forze.application.contracts.authn import AuthnSpec
from forze.base.exceptions import exc

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True, repr=False)
class CookieTokenAuthn:
    """Authentication ingress method for cookie-based token authentication."""

    authn_spec: AuthnSpec
    """Authentication spec to dispatch through."""

    cookie_name: str
    """Cookie name carrying the access token."""

    scheme: str = "Bearer"
    """Scheme label stored on :class:`AccessTokenCredentials`."""

    required: bool = False
    """Whether a missing cookie should raise :class:`AuthenticationError`."""

    description: str | None = None
    """Human-readable description of the ingress method (informational only)."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True, repr=False)
class HeaderTokenAuthn:
    """Authentication ingress method for header-based token authentication."""

    authn_spec: AuthnSpec
    """Authentication spec to dispatch through."""

    header_name: str
    """Header name carrying the bearer token."""

    required: bool = False
    """Whether a missing header should raise :class:`AuthenticationError`."""

    description: str | None = None
    """Human-readable description of the ingress method (informational only)."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True, repr=False)
class HeaderApiKeyAuthn:
    """Authentication ingress method for header-based API key authentication."""

    authn_spec: AuthnSpec
    """Authentication spec to dispatch through."""

    header_name: str
    """Header name carrying the API key."""

    required: bool = False
    """Whether a missing header should raise :class:`AuthenticationError`."""

    description: str | None = None
    """Human-readable description of the ingress method (informational only)."""


# ....................... #

AuthnIngress = CookieTokenAuthn | HeaderTokenAuthn | HeaderApiKeyAuthn
"""Authentication ingress methods."""

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True, repr=False)
class AuthnRequirement:
    """Authentication requirements."""

    ingress: tuple[AuthnIngress, ...]
    """Authentication ingress methods."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if not self.ingress:
            raise exc.internal("At least one ingress method should be provided")
