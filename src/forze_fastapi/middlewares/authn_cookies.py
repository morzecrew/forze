from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from collections.abc import Iterable, Mapping
from typing import Any, Final, cast

from forze.base.exceptions import exc

from ._routes import iter_effective_routes
from .security import is_anonymous_path

# ----------------------- #

AUTHN_COOKIE_LOGOUT_ATTR: Final[str] = "__forze_authn_cookie_logout__"
"""Marker a cookie-mode logout endpoint carries (set by ``attach_authn_routes``): the
carrier's ``(access_path, refresh_path)``, which :func:`check_authn_cookie_paths` holds
against the route."""

AUTHN_COOKIE_ANONYMOUS_ATTR: Final[str] = "__forze_authn_cookie_anonymous__"
"""Marker the cookie-mode login, refresh and logout endpoints carry (set by
``attach_authn_routes``): the routes :func:`check_authn_cookie_paths` requires in every
security middleware's ``anonymous_paths``."""


def check_authn_cookie_paths(app: Any) -> None:
    """Refuse a cookie-mode logout route the refresh cookie's path does not cover, and a
    cookie-mode login, refresh or logout route missing from ``anonymous_paths``.

    Cookie-mode logout revokes the session the access cookie's identity or, without one,
    the refresh cookie names. The access cookie expires first, so only the refresh cookie
    reaches logout for the whole session: a ``refresh_path`` that misses the mounted
    logout path (``/auth/refresh`` with logout at ``/auth/logout``) would answer a logout
    after the access cookie expires with 204 and revoke nothing.

    The browser keeps sending a stale access cookie to login, refresh and logout, so a
    :class:`~forze_fastapi.middlewares.SecurityContextMiddleware` 401s each one its
    ``anonymous_paths`` misses before the route runs: logout then neither revokes from the
    refresh cookie nor clears the cookies. Each must be listed on every such middleware, as
    the middleware reads the list; an app without one has nothing that 401s them.

    The path is the effective one, every router prefix applied, under the app's own
    ``root_path`` — the only root path known here: behind a server-level one
    (``--root-path``) a full-path ``anonymous_paths`` entry is refused, so list the path
    relative to it or declare it as ``FastAPI(root_path=...)``. A route inside a mounted
    sub-application is not seen from here, as for
    :func:`~forze_fastapi.middlewares.check_bypass_paths`, and neither is a security
    middleware on one. The middleware is found by its ``anonymous_paths`` field among the
    app's own middlewares, so one wrapped in a custom middleware or factory is not seen
    either. ``runtime_lifespan`` runs it at startup; an app with its own lifespan calls it
    itself.

    :raises CoreException: ``configuration`` when the refresh cookie's path does not
        cover a cookie-mode logout route, or a security middleware's ``anonymous_paths``
        misses a cookie-mode login, refresh or logout route.
    """

    root = (getattr(app, "root_path", "") or "").rstrip("/")
    gates = _anonymous_path_sets(app)

    for path, route in iter_effective_routes(app):
        endpoint = getattr(route, "endpoint", None)
        scope = {"path": root + path, "root_path": root}

        if getattr(endpoint, AUTHN_COOKIE_ANONYMOUS_ATTR, False) and not all(
            is_anonymous_path(paths, scope) for paths in gates
        ):
            raise exc.configuration(
                f"The cookie-mode authn route {root + path!r} is not in the security "
                "middleware's anonymous_paths, so a stale access cookie 401s it before it "
                "runs: logout would neither revoke the session from the refresh cookie nor "
                "clear the cookies. List the mounted login, refresh and logout paths in "
                "SecurityContextMiddleware(anonymous_paths=...). Behind a proxy that strips "
                "a prefix, declare it as FastAPI(root_path=...)."
            )

        cookie_paths = getattr(endpoint, AUTHN_COOKIE_LOGOUT_ATTR, ())

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


def _anonymous_path_sets(app: Any) -> list[frozenset[str]]:
    """The ``anonymous_paths`` of each middleware declaring the field.

    Detected structurally, as ``check_bypass_paths`` finds its gates: a middleware added
    without the kwarg still 401s every path.
    """

    sets: list[frozenset[str]] = []

    for middleware in getattr(app, "user_middleware", ()):
        cls = getattr(middleware, "cls", None)
        fields = getattr(cls, "__attrs_attrs__", ())

        if not any(getattr(field, "name", "") == "anonymous_paths" for field in fields):
            continue

        kwargs = cast("Mapping[str, Any]", getattr(middleware, "kwargs", None) or {})
        sets.append(frozenset(cast("Iterable[str]", kwargs.get("anonymous_paths") or ())))

    return sets


def _path_matches(cookie_path: str, path: str) -> bool:
    """Whether a browser sends a cookie scoped to *cookie_path* to *path* (RFC 6265 §5.1.4)."""

    # A path not starting with "/" is ignored: the browser scopes to the request's directory.
    if not cookie_path.startswith("/") or not path.startswith(cookie_path):
        return False

    return (
        len(path) == len(cookie_path) or cookie_path.endswith("/") or path[len(cookie_path)] == "/"
    )
