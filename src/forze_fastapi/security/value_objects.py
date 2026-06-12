"""Auth requirement value object for HTTP transport policies."""

import re
from typing import Any, final

import attrs

from forze.application.contracts.authn import AuthnSpec
from forze.base.exceptions import exc

# ----------------------- #

OpenApiSecurityScheme = tuple[str, dict[str, Any]]
"""A named OpenAPI ``securityScheme``: ``(scheme name, scheme object)``."""


def _sanitize_scheme_name(value: str) -> str:
    """Coerce a header/cookie name into an OpenAPI security-scheme key.

    Scheme keys are referenced from ``security`` requirements and must match
    ``^[a-zA-Z0-9._-]+$``; header names already mostly do, but anything else
    (whitespace, separators) is replaced so the key stays valid.
    """

    return re.sub(r"[^A-Za-z0-9._-]+", "_", value)


def _with_description(scheme: dict[str, Any], description: str | None) -> dict[str, Any]:
    """Attach the ingress description to a scheme object when one is set."""

    if description:
        scheme["description"] = description

    return scheme


# ....................... #


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

    def openapi_scheme(self) -> OpenApiSecurityScheme:
        """Project this ingress onto an OpenAPI ``securityScheme``.

        A cookie-borne token has no native OpenAPI bearer shape, so it is
        represented as an ``apiKey`` carried ``in: cookie`` (the conventional
        encoding).
        """

        name = f"cookieToken_{_sanitize_scheme_name(self.cookie_name)}"

        return name, _with_description(
            {"type": "apiKey", "in": "cookie", "name": self.cookie_name},
            self.description,
        )


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

    def openapi_scheme(self) -> OpenApiSecurityScheme:
        """Project this ingress onto an OpenAPI ``securityScheme``.

        A token on the ``Authorization`` header is the standard HTTP ``bearer``
        scheme; a token on any other header is an ``apiKey`` carried in that
        header.
        """

        if self.header_name.lower() == "authorization":
            return "bearerAuth", _with_description(
                {"type": "http", "scheme": "bearer"},
                self.description,
            )

        name = f"tokenHeader_{_sanitize_scheme_name(self.header_name)}"

        return name, _with_description(
            {"type": "apiKey", "in": "header", "name": self.header_name},
            self.description,
        )


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

    def openapi_scheme(self) -> OpenApiSecurityScheme:
        """Project this ingress onto an OpenAPI ``apiKey`` security scheme."""

        name = f"apiKey_{_sanitize_scheme_name(self.header_name)}"

        return name, _with_description(
            {"type": "apiKey", "in": "header", "name": self.header_name},
            self.description,
        )


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
