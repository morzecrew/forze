from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from typing import Any, Final

from forze.base.exceptions import exc

from ._routes import iter_effective_routes

# ----------------------- #

AUTHN_COOKIE_LOGOUT_ATTR: Final[str] = "__forze_authn_cookie_logout__"
"""Marker a cookie-mode logout endpoint carries (set by ``attach_authn_routes``): the
carrier's ``(access_path, refresh_path)``, which :func:`check_authn_cookie_paths` holds
against the route."""


def check_authn_cookie_paths(app: Any) -> None:
    """Refuse a cookie-mode logout route the refresh cookie's path does not cover.

    Cookie-mode logout revokes the session the access cookie's identity or, without one,
    the refresh cookie names. The access cookie expires first, so only the refresh cookie
    reaches logout for the whole session: a ``refresh_path`` that misses the mounted
    logout path (``/auth/refresh`` with logout at ``/auth/logout``) would answer a logout
    after the access cookie expires with 204 and revoke nothing.

    The path is the effective one, every router prefix applied, under the app's own
    ``root_path``. A route inside a mounted sub-application is not seen from here, as
    for :func:`~forze_fastapi.middlewares.check_bypass_paths`. ``runtime_lifespan`` runs
    it at startup; an app with its own lifespan calls it itself.

    :raises CoreException: ``configuration`` when the refresh cookie's path does not
        cover a cookie-mode logout route.
    """

    root = (getattr(app, "root_path", "") or "").rstrip("/")

    for path, route in iter_effective_routes(app):
        cookie_paths = getattr(getattr(route, "endpoint", None), AUTHN_COOKIE_LOGOUT_ATTR, ())

        if not cookie_paths:
            continue

        _, refresh_path = cookie_paths

        if _path_matches(refresh_path, root + path):
            continue

        raise exc.configuration(
            f"The refresh cookie is scoped to {refresh_path!r}, which does not cover the "
            f"cookie-mode logout route {root + path!r}: once the access cookie expires, the "
            "browser sends logout no cookie, so it cannot revoke the session. Widen "
            "AuthnCookieCarrier(refresh_path=...) to cover the logout route (e.g. the authn "
            "routes' prefix). Behind a proxy that strips a prefix, declare it as "
            "FastAPI(root_path=...)."
        )


def _path_matches(cookie_path: str, path: str) -> bool:
    """Whether a browser sends a cookie scoped to *cookie_path* to *path* (RFC 6265 §5.1.4)."""

    # A path not starting with "/" is ignored: the browser scopes to the request's directory.
    if not cookie_path.startswith("/") or not path.startswith(cookie_path):
        return False

    return (
        len(path) == len(cookie_path) or cookie_path.endswith("/") or path[len(cookie_path)] == "/"
    )
