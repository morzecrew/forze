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
    """Refuse a cookie-mode logout route neither authn cookie's path covers.

    Cookie-mode logout revokes the session the access cookie's identity or, without one,
    the refresh cookie names, so the browser must send one of them there. A carrier whose
    ``access_path`` and ``refresh_path`` both miss the mounted logout path (``/api`` and
    ``/auth/refresh`` with logout at ``/auth/logout``) would answer every logout with 204
    and revoke nothing.

    The path is the effective one, every router prefix applied, under the app's own
    ``root_path``. A route inside a mounted sub-application is not seen from here, as
    for :func:`~forze_fastapi.middlewares.check_bypass_paths`. ``runtime_lifespan`` runs
    it at startup; an app with its own lifespan calls it itself.

    :raises CoreException: ``configuration`` when neither cookie's path covers a
        cookie-mode logout route.
    """

    root = (getattr(app, "root_path", "") or "").rstrip("/")

    for path, route in iter_effective_routes(app):
        cookie_paths = getattr(getattr(route, "endpoint", None), AUTHN_COOKIE_LOGOUT_ATTR, ())

        if not cookie_paths or any(_path_matches(p, root + path) for p in cookie_paths):
            continue

        access_path, refresh_path = cookie_paths

        raise exc.configuration(
            f"The authn cookies are scoped to {access_path!r} (access) and {refresh_path!r} "
            f"(refresh), and neither covers the cookie-mode logout route {root + path!r}: "
            "the browser sends logout no cookie, so it cannot revoke the session. Widen "
            "AuthnCookieCarrier(refresh_path=...) to cover the logout route (e.g. the authn "
            "routes' prefix), or mount the authn routes under the access cookie's path. "
            "Behind a proxy that strips a prefix, declare it as FastAPI(root_path=...)."
        )


def _path_matches(cookie_path: str, path: str) -> bool:
    """Whether a browser sends a cookie scoped to *cookie_path* to *path* (RFC 6265 §5.1.4)."""

    if not path.startswith(cookie_path):
        return False

    return (
        len(path) == len(cookie_path) or cookie_path.endswith("/") or path[len(cookie_path)] == "/"
    )
