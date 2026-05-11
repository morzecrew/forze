from typing import Literal, NotRequired, TypedDict

from ..http import SimpleHttpEndpointSpec

# ----------------------- #


CookieSameSite = Literal["lax", "strict", "none"]
"""SameSite values supported on Set-Cookie response headers."""


# ....................... #


class HeaderTokenTransportSpec(TypedDict, total=False):
    """Transport configuration for tokens carried in an HTTP header."""

    kind: Literal["header"]
    header_name: str
    """Header name carrying the token (e.g. ``Authorization`` for access tokens)."""

    scheme: str
    """OAuth2-style scheme label, e.g. ``Bearer``. Used both for the ``Authorization``
    header value (``f"{scheme} {token}"``) and as the response body's
    ``access_token_type``. Defaults to the value carried on
    :class:`~forze.application.contracts.authn.AccessTokenCredentials`."""


# ....................... #


class CookieTokenTransportSpec(TypedDict, total=False):
    """Transport configuration for tokens carried in an HTTP cookie."""

    kind: Literal["cookie"]
    cookie_name: str
    """Cookie name carrying the token (required when ``kind == "cookie"``)."""

    cookie_secure: bool
    """``Secure`` flag (default ``True``). Set ``False`` only for local dev."""

    cookie_http_only: bool
    """``HttpOnly`` flag (default ``True``). Strongly recommended."""

    cookie_samesite: CookieSameSite
    """``SameSite`` flag (default ``lax``)."""

    cookie_path: str
    """``Path`` (default ``/``)."""

    cookie_domain: str | None
    """``Domain`` (default ``None`` meaning host-only cookie)."""

    cookie_max_age_from_lifetime: bool
    """Use the issued token's ``expires_in`` as ``Max-Age`` (default ``True``).
    When ``False`` no ``Max-Age``/``Expires`` is set (session cookie)."""


# ....................... #


TokenTransportSpec = HeaderTokenTransportSpec | CookieTokenTransportSpec
"""Either a header or cookie transport descriptor."""


# ....................... #


class AuthnConfigSpec(TypedDict, total=False):
    """Shared configuration block applied to all authn endpoints on a router."""

    access_token_transport: TokenTransportSpec
    """How the access token is sent back. Default:
    ``{"kind": "header", "header_name": "Authorization", "scheme": "Bearer"}``."""

    refresh_token_transport: TokenTransportSpec
    """How the refresh token is sent back AND read on the refresh endpoint.
    Default mirrors ``access_token_transport``."""


# ....................... #


class AuthnEndpointsSpec(TypedDict, total=False):
    """Per-endpoint toggles for the authn endpoint scaffold.

    Each entry mirrors the document/search shape: ``False`` (default) skips the
    endpoint, ``True`` enables it with default :class:`SimpleHttpEndpointSpec`,
    and a :class:`SimpleHttpEndpointSpec` allows full customisation
    (path/metadata/authn requirement).
    """

    password_login: SimpleHttpEndpointSpec | bool
    refresh: SimpleHttpEndpointSpec | bool
    logout: SimpleHttpEndpointSpec | bool
    change_password: SimpleHttpEndpointSpec | bool

    # ....................... #

    config: NotRequired[AuthnConfigSpec]
