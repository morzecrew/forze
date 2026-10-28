from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from collections.abc import Callable, Iterator, Set
from functools import partial
from typing import Any

from fastapi.routing import iter_route_contexts
from starlette.applications import Starlette
from starlette.routing import Host, Mount, Router
from starlette.staticfiles import StaticFiles
from starlette.types import Scope

from forze.base.exceptions import exc

# ----------------------- #


def path_is_listed(paths: Set[str], scope: Scope) -> bool:
    """Whether the request path is listed in *paths*, as the gating middlewares read their
    ``anonymous_paths``, ``bypass_paths`` and ``allowed_websocket_paths``.

    An entry is the full path, or the path relative to ``scope["root_path"]`` — a mount
    prefix or a server root path. Reads the percent-decoded ``scope["path"]`` routing
    reads, never ``request.url``: that is re-split from it, so an encoded ``?`` or ``#``
    would end its path early and borrow a listed one.

    The relative reading trusts ``scope["root_path"]``: it must come from server
    configuration (a ``Mount``, ``--root-path``), never from a request header. A
    middleware that set it from ``X-Forwarded-Prefix`` would let a client pick the prefix,
    and a relative entry would then admit paths the startup checks never verified.
    """

    path: str = scope.get("path", "")

    if path in paths:
        return True

    # Strip the mount exactly as Starlette's router does, so the relative path is
    # the one the request is routed by — never a different route.
    root: str = scope.get("root_path", "")

    if not root or not path.startswith(root) or path[len(root) : len(root) + 1] != "/":
        return False

    relative = path[len(root) :]

    # Led by the prefix's path segments, it could also be another route's full path:
    # an entry that reads both ways only ever counts as full (checked above).
    if relative == root or relative.startswith(root + "/"):
        return False

    return relative in paths


# ....................... #


def iter_effective_routes(app: Any) -> Iterator[tuple[str, Any]]:
    """Every route an app serves, each with the path it is actually reachable at.

    The startup checks compare configured paths against what the app really routes,
    so they need the **mounted** path — every router prefix applied, not just the
    innermost one. A route's own ``path`` is not that: ``iter_route_contexts``
    flattens nested includes, and for an included route ``route.path`` keeps only the
    prefix chain of the router it was declared on. Include ``/v1`` into ``/api`` and
    an HTTP route reports ``/v1/livez`` while serving ``/api/v1/livez`` — a check
    reading it would refuse a correct configuration and, worse, fail to recognise a
    governed route someone bypassed.

    The effective path lives in a different place per route kind, which is why it is
    resolved here once rather than at each call site:

    - **HTTP routes** carry it on the include context (``_route_context.path``); the
      context's ``starlette_route`` is unset.
    - **Websocket routes** carry it on ``_route_context.starlette_route``, whose own
      ``path`` is the mounted one; the context's ``path`` is empty for them.
    - **Top-level routes** have no include context at all, and their own ``path`` is
      already the full one.

    ``iter_route_contexts`` yields a ``Mount`` (or ``Host``) as one opaque route. A mounted
    Starlette or FastAPI app, or a router, is descended into, its routes under the mount
    prefix; any other mounted ASGI app cannot be seen into and is yielded as the mount.

    :returns: ``(effective path, route)`` pairs — the route being the object that
        actually serves it, so callers read ``endpoint`` and match types on it.
    """

    for path, route in _iter_own_routes(app):
        mounted = _mounted_app(route)

        if mounted is None:
            yield path, route

            continue

        for sub_path, sub_route in iter_effective_routes(mounted):
            yield path + sub_path, sub_route


def iter_mounted_apps(app: Any) -> Iterator[tuple[Any, Callable[[str], list[Scope]]]]:
    """*app* and every Starlette or FastAPI app mounted beneath it, each with the scopes its
    own middlewares see for a request to one of its routes.

    Starlette never sends lifespan events to a mounted app, so its startup checks run
    from the outermost one. A ``Mount`` keeps the full path in ``scope["path"]`` and
    extends ``root_path`` by its prefix, so a middleware on an app mounted at ``/api``
    sees ``/api/livez`` under the root path ``/api``. On the outermost app, the only root
    path known is its own ``root_path``: under it the middleware sees the path with the
    root path, or without it behind a proxy that strips it.

    :returns: ``(app, scopes)`` pairs, where ``scopes(path)`` gives the scopes the app's
        middlewares see for a request to *path*, as :func:`iter_effective_routes` yields
        it for that app.
    :raises CoreException: ``configuration`` when a mounted FastAPI app declares a
        ``root_path`` other than the one its mount passes down, which it would overwrite.
    """

    raw_root = getattr(app, "root_path", "") or ""
    root = raw_root.rstrip("/")
    bases = (root, "") if raw_root else (root,)

    yield app, partial(_scopes, bases, raw_root)
    yield from _iter_mounted_beneath(app, root, raw_root)


def _iter_mounted_beneath(
    node: Any,
    base: str,
    root_path: str,
) -> Iterator[tuple[Any, Callable[[str], list[Scope]]]]:
    for path, route in _iter_own_routes(node):
        mounted = _mounted_app(route)

        if mounted is None:
            continue

        if not isinstance(mounted, Starlette):
            yield from _iter_mounted_beneath(mounted, base + path, root_path + path)

            continue

        sub_root = root_path + path
        own_root = getattr(mounted, "root_path", "") or sub_root

        # FastAPI writes its own root_path over the one the mount passed down, and its
        # router then strips that prefix instead: not what these scopes model.
        if own_root != sub_root:
            raise exc.configuration(
                f"The application mounted at {path or '/'!r} declares "
                f"root_path={own_root!r}, which replaces the {sub_root!r} its mount passes "
                "down: its routes are matched against another prefix, and the startup "
                "checks cannot tell what its middlewares see. Drop root_path from the "
                "mounted application; the mount sets it."
            )

        yield mounted, partial(_scopes, (base + path,), sub_root)
        yield from _iter_mounted_beneath(mounted, base + path, sub_root)


def entry_into_opaque_mount(
    paths: Set[str],
    app: Any,
    scopes: Callable[[str], list[Scope]],
) -> tuple[str, str] | None:
    """The first entry of *paths* that admits a request into a mounted ASGI app the checks
    cannot see into (neither a Starlette or FastAPI app nor a router), with the mount's path.

    ``StaticFiles`` is not one: it serves files, no governed operation, and closes every
    websocket scope. Routing order is not modelled: an entry that could reach such a mount
    counts even when a route declared before it would serve the request.
    """

    mounts = [
        (path, route)
        for path, route in iter_effective_routes(app)
        if isinstance(route, (Mount, Host)) and not isinstance(route.app, StaticFiles)
    ]

    if not mounts:
        return None

    bases = [scope["path"] for scope in scopes("")]

    for entry in sorted(paths):
        # The route paths a request the entry lists is routed by: the entry read relative,
        # or the entry less the base read full.
        routed = {entry} | {entry[len(b) :] for b in bases if b and entry.startswith(b + "/")}

        for mount_path, route in mounts:
            for path in routed:
                if _routes_into(mount_path, route, path) and any(
                    path_is_listed({entry}, scope) for scope in scopes(path)
                ):
                    return entry, mount_path or "/"

    return None


def _routes_into(mount_path: str, route: Any, path: str) -> bool:
    """Whether a request routed by *path* can reach the mount or host route."""

    # A host route matches any path, and so does a mount at the root or one with params.
    if isinstance(route, Host) or not mount_path or "{" in mount_path:
        return True

    return path.startswith(mount_path + "/")


def _scopes(bases: tuple[str, ...], root_path: str, path: str) -> list[Scope]:
    return [{"path": base + path, "root_path": root_path} for base in bases]


def _mounted_app(route: Any) -> Any | None:
    """The app or router a mount routes into, when it can be seen into."""

    if not isinstance(route, (Mount, Host)):
        return None

    app = route.app

    return app if isinstance(app, (Starlette, Router)) else None


def _iter_own_routes(app: Any) -> Iterator[tuple[str, Any]]:
    """The app's routes, included routers flattened, mounts not descended into."""

    for context in iter_route_contexts(list(getattr(app, "routes", ()))):
        route_context = getattr(context, "_route_context", None)
        starlette_route = getattr(route_context, "starlette_route", None)

        if starlette_route is not None:
            yield getattr(starlette_route, "path", ""), starlette_route

            continue

        # An empty context path means the context carries no path for this route
        # kind, not that the route is mounted at the root.
        context_path = getattr(route_context, "path", "") or ""

        route: Any = context.route

        yield context_path or getattr(route, "path", ""), route
