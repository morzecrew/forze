from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from collections.abc import Iterable, Mapping
from typing import Any, Final, cast

from starlette.routing import Route

from forze.base.exceptions import exc

from ._routes import iter_effective_routes

# ----------------------- #

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
      mounted under a prefix while the set names the route-local paths (list
      ``/api/livez``, not ``/livez``); routes inside a mounted *sub-application* are
      likewise invisible here and belong on that app's own middlewares. A superset is
      fine and expected —
      ``DEFAULT_HEALTH_PATHS`` names ten paths and most apps serve three — but a set
      that matches *nothing* bypasses nothing, and the probe it was added for goes on
      failing exactly as it did before.

    A no-op when no middleware configures a bypass.
    """

    # A gating middleware is detected structurally (it declares the field), the same
    # way check_websocket_allowlist finds them: a middleware added without the kwarg
    # still runs on every request, so presence of the kwarg alone would miss it.
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

    bypassed: set[str] = set()

    for _, paths in gates:
        bypassed.update(paths)

    if not bypassed:
        return

    _check_gates_agree(gates)

    governed: dict[str, str] = {}
    served: set[str] = set()

    for route in iter_effective_routes(app):
        if not isinstance(route, Route):
            continue

        served.add(route.path)

        if getattr(route.endpoint, GOVERNED_OPERATION_ATTR, False):
            governed[route.path] = getattr(route, "name", route.path)

    for path in sorted(bypassed & governed.keys()):
        raise exc.configuration(
            f"bypass_paths lists {path!r}, but that path serves the generated "
            f"operation route {governed[path]!r}. A bypassed path runs with no "
            "identity and no tenant bound, so the operation would read and write "
            "tenant data unauthenticated. List probe and scrape paths only."
        )

    if not bypassed & served:
        raise exc.configuration(
            f"bypass_paths lists {sorted(bypassed)}, and not one of those paths is "
            "routed by this app — nothing is bypassed and the probe it was added for "
            "still fails. The usual cause is a router mounted under a prefix: the "
            "middlewares run before routing, so list the full mounted path "
            "(/api/livez, not /livez). A path inside a mounted sub-application cannot "
            "be seen from here — bypass it on that application's own middlewares. Drop "
            "the argument if the probes are served elsewhere entirely."
        )


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
