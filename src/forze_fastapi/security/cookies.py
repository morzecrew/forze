"""Cookie carrier for authn tokens — the Set-Cookie half of cookie-mode sessions.

:class:`CookieTokenAuthn` reads an access token *inbound*; this is its outbound
twin: one declared policy object that **sets** the cookies on login/refresh,
**clears** them on logout, **reads** the refresh token back on refresh, and strips
token strings from response bodies (the OAuth2-shaped body keeps scheme + lifetime,
so cookie clients still learn how long the credential is valid — exactly the split
:class:`~forze_kits.aggregates.authn.AuthnTokenResponseDTO` documents).

Wire it into :func:`~forze_fastapi.routes.attach_authn_routes` via ``cookies=`` and
point :class:`CookieTokenAuthn`'s ``cookie_name`` at :attr:`access_cookie`. Scope
the two cookies with :attr:`access_path` / :attr:`refresh_path` (e.g. access →
``/api``, refresh → ``/auth/refresh``) so the refresh token never rides ordinary
API requests — and pair the login/refresh paths with the security middleware's
``anonymous_paths``, or a stale access cookie 401s the very route that replaces it.
"""

from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from typing import Literal, final

import attrs
from fastapi import Request, Response

from forze_kits.aggregates.authn import AuthnTokenResponseDTO

# ----------------------- #

__all__ = ["AuthnCookieCarrier"]


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class AuthnCookieCarrier:
    """Set / rotate / clear authn token cookies, and strip tokens from bodies.

    Cookies are ``HttpOnly`` always (a script-readable session token defeats the
    point of cookie mode) and ``Secure`` by default. ``Max-Age`` comes from the
    token response's own lifetimes (``access_expires_in`` / ``refresh_expires_in``);
    a token without a known lifetime becomes a session cookie.

    **CSRF posture.** Two independent layers. Outbound, the default
    ``samesite="lax"`` (or ``"strict"``) keeps browsers from attaching these
    cookies to cross-site requests. Inbound, :class:`CookieTokenAuthn`'s
    :class:`~forze_fastapi.security.CookieCsrf` gate (on by default) requires an
    unsafe request using the cookie to prove a same-host or allowlisted origin —
    the server-side check that still holds when a proxy strips ``SameSite`` or a
    ``samesite="none"`` deployment relies on it; list your cross-origin frontends
    in its ``allowed_origins``. The carrier still ships no double-submit token: a
    token needs an issuance surface (a page or bootstrap route) that the authn
    routes do not own.
    """

    access_cookie: str = "forze_access"
    """Access-token cookie name — point :class:`CookieTokenAuthn` at the same name."""

    refresh_cookie: str = "forze_refresh"
    """Refresh-token cookie name."""

    access_path: str = "/"
    """Path scope of the access cookie (e.g. ``/api``)."""

    refresh_path: str = "/"
    """Path scope of the refresh cookie. Scope it to the refresh route (e.g.
    ``/auth/refresh``) so the long-lived credential rides exactly one request shape."""

    domain: str | None = None
    """Cookie ``Domain``; ``None`` = host-only."""

    secure: bool = True
    """``Secure`` flag — HTTPS-only cookies. Disable only for local development."""

    samesite: Literal["lax", "strict", "none"] = "lax"
    """``SameSite`` policy. ``"none"`` requires ``secure=True`` (browsers enforce it)."""

    strip_body_tokens: bool = True
    """Remove token strings from the response body once they ride cookies. The body
    keeps ``access_token_type`` and the ``*_expires_in`` lifetimes."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.samesite == "none" and not self.secure:
            from forze.base.exceptions import exc

            raise exc.configuration(
                "SameSite=None cookies require secure=True — browsers refuse them "
                "over plain HTTP, which would silently drop the session.",
            )

    # ....................... #

    def apply(self, response: Response, tokens: AuthnTokenResponseDTO) -> AuthnTokenResponseDTO:
        """Set (or rotate) both cookies from a token response; return the body to send.

        Called on login **and** refresh — a rotated refresh token overwrites its
        cookie, which is what makes single-use rotation work in cookie mode.
        """

        if tokens.access_token is not None:
            response.set_cookie(
                key=self.access_cookie,
                value=tokens.access_token,
                max_age=tokens.access_expires_in,
                path=self.access_path,
                domain=self.domain,
                secure=self.secure,
                httponly=True,
                samesite=self.samesite,
            )

        if tokens.refresh_token is not None:
            response.set_cookie(
                key=self.refresh_cookie,
                value=tokens.refresh_token,
                max_age=tokens.refresh_expires_in,
                path=self.refresh_path,
                domain=self.domain,
                secure=self.secure,
                httponly=True,
                samesite=self.samesite,
            )

        if not self.strip_body_tokens:
            return tokens

        # nosec B105 — these are field NAMES being cleared to None (the opposite of a
        # hardcoded credential: the tokens are REMOVED from the body).
        return tokens.model_copy(
            update={"access_token": None, "refresh_token": None}  # nosec B105
        )

    # ....................... #

    def clear(self, response: Response) -> None:
        """Expire both cookies — the logout half of the carrier.

        The delete must repeat the scoping attributes (path/domain) or browsers
        treat it as a *different* cookie and keep the credential alive.
        """

        response.delete_cookie(
            key=self.access_cookie,
            path=self.access_path,
            domain=self.domain,
            secure=self.secure,
            httponly=True,
            samesite=self.samesite,
        )
        response.delete_cookie(
            key=self.refresh_cookie,
            path=self.refresh_path,
            domain=self.domain,
            secure=self.secure,
            httponly=True,
            samesite=self.samesite,
        )

    # ....................... #

    def read_refresh(self, request: Request) -> str | None:
        """The refresh token from its cookie, or ``None`` when absent."""

        return request.cookies.get(self.refresh_cookie) or None
