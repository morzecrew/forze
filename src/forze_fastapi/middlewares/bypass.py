from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from collections.abc import Callable, Iterable, Mapping
from typing import Any, Final, cast

from starlette.routing import Host, Mount, Route
from starlette.types import Scope

from forze.base.exceptions import exc
from forze.base.logging import Logger

from ._routes import iter_effective_routes, iter_mounted_apps, path_is_listed

# ----------------------- #

_logger = Logger("fastapi.startup")

GOVERNED_OPERATION_ATTR: Final[str] = "__forze_governed_operation__"
"""Marker a generated operation route's endpoint carries (set by ``attach_*_routes``).

What :func:`check_bypass_paths` refuses to see behind a bypassed path: these routes
run registry operations against tenant data, and the middlewares they would skip are
what bind the identity and tenant those operations read.
"""


def check_bypass_paths(app: Any) -> None:
    """Reconcile the middlewares' ``bypass_paths`` against the app's real routes.

    The middlewares run before routing, so a configured path can only assert a
    *path*. This check, run once at startup, proves that what it names is both real
    and safe to leave ungoverned. Three ways to fail, all closed:

    - **A bypassed path serves a generated operation route.** Those read and write
      tenant data through the registry, and bypassing means no identity and no tenant
      is bound for them — the one outcome no probe path needs and no data route
      survives. Hand-written routes are never judged: forze refuses only to disarm
      governance on routes it generated as governed.
    - **The two gating middlewares carry different sets.** Both resolve the execution
      context, so a path only one of them skips is still resolved by the other and
      still fails the same way — the bypass reads as configured and does nothing.
    - **Nothing in a non-empty set matches any route.** The usual cause is a router
      included under a prefix while the set names the route-local paths (list
      ``/api/livez``, not ``/livez``). A superset is fine and expected —
      ``DEFAULT_HEALTH_PATHS`` names ten paths and most apps serve three — but a set
      that matches *nothing* bypasses nothing, and the probe it was added for goes on
      failing exactly as it did before.

    Run it on the outermost app: it checks that app and every Starlette or FastAPI app
    mounted beneath it, each one's middlewares against the routes they reach, read as
    they read them — the full path, or the path relative to the app's mount. A request
    is judged under every path its middleware may see (with and without the app's own
    ``root_path``): bypassed under any of them counts for the governed-route refusal,
    and only bypassed under all of them counts as a match. A bypassed path inside any
    other mounted ASGI app cannot be verified and is logged as a warning.

    A no-op when no middleware configures a bypass.
    :func:`~forze_fastapi.run_startup_checks` runs it (``runtime_lifespan`` calls that) at
    startup; an app with its own lifespan calls ``run_startup_checks`` itself, or it opts
    out of every startup check these gates rely on.
    """

    for level, scopes in iter_mounted_apps(app):
        _check_level(level, scopes)


def _check_level(app: Any, scopes: Callable[[str], list[Scope]]) -> None:
    """One app's gates against the HTTP routes it reaches."""

    gates = _gating_middlewares(app)
    bypassed = {path for _, paths in gates for path in paths}

    if not bypassed:
        return

    _check_gates_agree(gates)

    routes: list[tuple[list[Scope], Any]] = []
    opaque: list[str] = []

    for path, route in iter_effective_routes(app):
        if isinstance(route, Route):
            routes.append((scopes(path), route))

        elif isinstance(route, (Mount, Host)):
            opaque.append(path)

    for seen, route in routes:
        if getattr(route.endpoint, GOVERNED_OPERATION_ATTR, False) and any(
            path_is_listed(bypassed, scope) for scope in seen
        ):
            raise exc.configuration(
                f"bypass_paths lists {sorted(_listing(bypassed, seen))}, which bypasses "
                f"{seen[0]['path']!r}, the generated operation route "
                f"{getattr(route, 'name', seen[0]['path'])!r}. A bypassed path runs with "
                "no identity and no tenant bound, so the operation would read and write "
                "tenant data unauthenticated. List probe and scrape paths only."
            )

    if not any(all(path_is_listed(bypassed, scope) for scope in seen) for seen, _ in routes):
        raise exc.configuration(
            f"bypass_paths lists {sorted(bypassed)}, and not one of those paths is "
            "routed by this app — nothing is bypassed and the probe it was added for "
            "still fails. The usual cause is a router included under a prefix: the "
            "middlewares run before routing, so list the full mounted path "
            "(/api/livez, not /livez); on an application mounted under a prefix, the full "
            "path or the path relative to the mount, and run the checks on the outermost "
            "app (run_startup_checks), the only one that sees the mount. Drop the argument "
            "if the probes are served elsewhere entirely."
        )

    _warn_unverifiable(bypassed, routes, opaque, scopes)


def _listing(paths: Iterable[str], seen: list[Scope]) -> set[str]:
    """The entries that list a request seen as any of *seen*."""

    return {path for path in paths if any(path_is_listed({path}, scope) for scope in seen)}


def _warn_unverifiable(
    bypassed: set[str],
    routes: list[tuple[list[Scope], Any]],
    opaque: list[str],
    scopes: Callable[[str], list[Scope]],
) -> None:
    """Warn about entries that reach only a mounted ASGI app the check cannot see into."""

    unmatched = bypassed - {path for seen, _ in routes for path in _listing(bypassed, seen)}

    for mount in opaque:
        prefixes = {mount, *(scope["path"] for scope in scopes(mount))}
        inside = sorted(
            path for path in unmatched if any(path.startswith(f"{p}/") for p in prefixes)
        )

        if inside:
            _logger.warning(
                "bypass_paths lists paths inside a mounted application the startup "
                "checks cannot inspect; they are not verified to be probe paths",
                paths=inside,
                mount=mount,
            )


def _gating_middlewares(app: Any) -> list[tuple[str, frozenset[str]]]:
    """Each middleware declaring the field, with the paths it was given.

    Detected structurally, the same way ``check_websocket_allowlist`` finds them: a
    middleware added without the kwarg still runs on every request, so presence of the
    kwarg alone would miss it.
    """

    gates: list[tuple[str, frozenset[str]]] = []

    for middleware in getattr(app, "user_middleware", ()):
        cls = getattr(middleware, "cls", None)
        fields = getattr(cls, "__attrs_attrs__", ())

        if not any(getattr(field, "name", "") == "bypass_paths" for field in fields):
            continue

        kwargs = cast("Mapping[str, Any]", getattr(middleware, "kwargs", None) or {})
        gates.append(
            (
                getattr(cls, "__name__", str(cls)),
                frozenset(cast("Iterable[str]", kwargs.get("bypass_paths") or ())),
            )
        )

    return gates


def _check_gates_agree(gates: list[tuple[str, frozenset[str]]]) -> None:
    """Every gating middleware must bypass the same paths."""

    for name, paths in gates:
        missing = {path for _, other in gates for path in other} - paths

        if missing:
            raise exc.configuration(
                f"bypass_paths differ between the gating middlewares: {sorted(missing)} "
                f"bypassed elsewhere but not on {name} — that middleware still resolves "
                "the execution context there, so the path fails exactly as it did "
                "without any bypass. List the same bypass_paths on every governed "
                "middleware."
            )
