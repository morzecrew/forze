"""Auth requirement value object for HTTP transport policies."""

import re
from typing import Any, final
from urllib.parse import urlsplit

import attrs

from forze.application.contracts.authn import AuthnSpec
from forze.base.exceptions import exc

# ----------------------- #

_SAFE_METHODS = frozenset({"GET", "HEAD", "OPTIONS", "TRACE"})
"""RFC 9110 safe methods — no server state change, so no cross-site forgery target."""

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
@attrs.define(slots=True, kw_only=True, frozen=True)
class CookieCsrf:
    """Server-side CSRF gate for a cookie ingress: origin proof on unsafe methods.

    The outbound carrier's ``SameSite`` is a browser default, not a server guarantee —
    a ``SameSite=None`` deployment, a proxy that strips the attribute, or a
    state-changing route on a nominally safe method reopens the cross-site path. This
    gate runs **before the cookie is read**: on an unsafe method the request must
    prove its origin via the ``Origin`` header (falling back to ``Referer`` for the
    browsers that omit ``Origin`` on same-origin form posts), and that origin must be
    the request's own host or an allowlisted one. Non-browser clients that send
    neither header should authenticate via a header ingress instead — or the
    deployment opts them in with :attr:`allow_missing_origin`.

    Safe methods (GET/HEAD/OPTIONS/TRACE) pass without proof by default: browsers do
    not send ``Origin`` on top-level same-origin navigation, so gating them would
    break every page load that carries the cookie. A route that mutates state on a
    safe method has no browser-compatible CSRF defense — fix the method.
    """

    allowed_origins: frozenset[str] = attrs.field(default=frozenset(), converter=frozenset)
    """Cross-origin callers allowed to use the cookie, as exact ``scheme://host[:port]``
    origins (e.g. a SPA on ``https://app.example.com`` calling this API's host). The
    request's own host is always allowed and need not be listed. Entries are validated
    at construction — a malformed origin here would otherwise silently never match."""

    allow_missing_origin: bool = False
    """Accept an unsafe request that carries neither ``Origin`` nor ``Referer``.

    Off (default) rejects it: a browser that can be CSRF'd sends at least one of the
    two, so the only callers this refuses are non-browser clients using the cookie —
    which a forged cross-site request cannot distinguish itself from."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        for allowed in self.allowed_origins:
            if _normalize_origin(allowed) is None or not urlsplit(allowed.strip()).scheme:
                raise exc.configuration(
                    f"allowed_origins entry {allowed!r} is not a valid scheme://host[:port] origin",
                )

    # ....................... #

    def rejection(
        self,
        *,
        method: str,
        host: str | None,
        origin: str | None,
        referer: str | None,
    ) -> str | None:
        """Why the request fails the gate, or ``None`` when it passes.

        Pure (headers in, verdict out) so the policy is testable without a request.
        *host* is the request's own host header value (``host[:port]``).

        The same-host comparison is by hostname **and port** (a same-origin browser
        request serializes both identically in ``Origin`` and ``Host``); the scheme is
        deliberately not compared — the ``Host`` header carries none, and the transport
        scheme is unreliable behind a TLS-terminating proxy. A deployment that must
        pin schemes lists exact origins in :attr:`allowed_origins`.
        """

        if method.upper() in _SAFE_METHODS:
            return None

        source = origin if origin is not None else referer

        if source is None or not source.strip():
            if self.allow_missing_origin:
                return None

            return "the request carries neither an Origin nor a Referer header"

        # A present-but-opaque or malformed origin ("null" from a sandboxed iframe,
        # a nonsense port) is attacker-adjacent input — never treated as missing,
        # never allowed to error out of the gate.
        source_authority = _authority(source)

        if source_authority is None:
            return f"the request origin {source.strip()!r} is opaque or malformed"

        if host is not None and source_authority == _authority(f"//{host}"):
            return None

        if _normalize_origin(source) in {
            _normalize_origin(allowed) for allowed in self.allowed_origins
        }:
            return None

        return f"the request origin {source.strip()!r} is not this host or an allowed origin"


def _authority(value: str) -> tuple[str, int | None] | None:
    """``(hostname, port)`` of an origin/URL/authority string, lowercased — or ``None``
    when there is no hostname or the port is malformed/out-of-range (``urlsplit``'s
    ``port`` raises ``ValueError`` there; a forged header must read as non-matching,
    never as a server error)."""

    parts = urlsplit(value.strip())

    try:
        hostname, port = parts.hostname, parts.port
    except ValueError:
        return None

    if hostname is None:
        return None

    return hostname, port


def _normalize_origin(value: str) -> str | None:
    """``scheme://host[:port]`` with the scheme/host lowercased and any path dropped
    (a ``Referer`` carries a full URL; comparing origins must ignore its path), or
    ``None`` for an unparseable value — which then matches nothing."""

    authority = _authority(value)

    if authority is None:
        return None

    hostname, port = authority

    return f"{urlsplit(value.strip()).scheme.lower()}://{hostname}" + (
        f":{port}" if port is not None else ""
    )


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

    csrf: CookieCsrf | None = attrs.field(factory=CookieCsrf)
    """Server-side CSRF gate, **on by default** (see :class:`CookieCsrf`): an unsafe
    method must prove a same-host or allowlisted origin before the cookie is read.
    ``None`` disables it — a declared decision that the deployment brings its own
    CSRF defense (a double-submit token), not just the carrier's ``SameSite``."""

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
