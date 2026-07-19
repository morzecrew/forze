from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from collections.abc import Iterable, Mapping
from typing import Any, Final, cast

from starlette.routing import WebSocketRoute
from starlette.types import Receive, Scope, Send

from forze.base.exceptions import exc

# ----------------------- #

GOVERNED_WEBSOCKET_ATTR: Final[str] = "__forze_governed_websocket__"
"""Marker attribute a governed websocket endpoint carries (set by its attach helper).

What :func:`check_websocket_allowlist` verifies an allowlisted path actually serves —
the allowlist alone proves only that a *path* was declared, not which endpoint routing
resolves there.
"""

WS_POLICY_VIOLATION: int = 1008
"""RFC 6455 close code sent when a raw websocket scope is refused."""


def websocket_scope_refused(
    scope: Scope,
    *,
    allow_raw_websockets: bool,
    allowed_websocket_paths: frozenset[str],
) -> bool:
    """The one gate both governed middlewares apply to a non-HTTP scope.

    A ``websocket`` scope passes only via the app-wide self-managed hatch or an
    exact-path allowlist entry (a governed route that resolves identity itself);
    everything else — including a *prefix* of an allowlisted path — is refused.
    Non-websocket scopes (``lifespan``, …) are never refused. Allowlist entries are
    the **full mounted path** (router prefixes included); run
    :func:`check_websocket_allowlist` at startup (``runtime_lifespan`` does) so a
    prefix mismatch or a non-governed route at an allowlisted path fails the boot
    instead of silently refusing — or serving — connections.
    """

    return (
        scope["type"] == "websocket"
        and not allow_raw_websockets
        and scope.get("path") not in allowed_websocket_paths
    )


def check_websocket_allowlist(app: Any) -> None:
    """Reconcile the middlewares' websocket allowlists against the app's real routes.

    The middlewares run before routing, so an allowlist entry can only assert a
    *path* — this check, run once at startup, proves what that path actually serves:
    every allowlisted path must resolve to exactly one websocket route whose endpoint
    carries the governed marker (set by ``attach_realtime_ws_route``). Fails closed
    (``configuration``) on a path with no websocket route (the usual cause: the
    allowlist carries the route-local path while the router is mounted under a
    prefix — allowlist the full mounted path), on a non-governed route at an
    allowlisted path (it would receive unauthenticated, tenant-free traffic), on
    duplicate websocket routes at one allowlisted path, and on an allowlist that
    differs **between** the gating middlewares — each gate enforces its own set at
    runtime, so a path one middleware allows and another does not would pass this
    check while the stricter gate refuses every connection (1008). A no-op when no
    allowlists are configured.
    """

    # A gating middleware is detected structurally (it declares the gate fields) —
    # one added without any websocket kwargs still refuses every websocket scope,
    # so presence of the kwargs alone would miss it.
    gates: list[tuple[str, bool, frozenset[str]]] = []

    for middleware in getattr(app, "user_middleware", ()):
        cls = getattr(middleware, "cls", None)
        fields = getattr(cls, "__attrs_attrs__", ())

        if not any(getattr(field, "name", "") == "allowed_websocket_paths" for field in fields):
            continue

        kwargs = cast("Mapping[str, Any]", getattr(middleware, "kwargs", None) or {})
        gates.append(
            (
                getattr(cls, "__name__", str(cls)),
                bool(kwargs.get("allow_raw_websockets", False)),
                frozenset(cast("Iterable[str]", kwargs.get("allowed_websocket_paths") or ())),
            )
        )

    allowlisted: set[str] = set()

    for _, _, paths in gates:
        allowlisted.update(paths)

    if not allowlisted:
        return

    for name, allow_raw, paths in gates:
        if allow_raw:
            continue  # this gate passes every websocket scope; it cannot reject

        missing = allowlisted - paths

        if missing:
            raise exc.configuration(
                f"allowed_websocket_paths differ between the gating middlewares: "
                f"{sorted(missing)} allowlisted elsewhere but not on {name} — that "
                "middleware would refuse every such connection (1008). List the same "
                "allowed_websocket_paths on every governed middleware."
            )

    websocket_routes: dict[str, list[Any]] = {}

    def _collect(route: Any) -> None:
        if isinstance(route, WebSocketRoute):
            websocket_routes.setdefault(route.path, []).append(route.endpoint)
            return

        # FastAPI keeps included routers nested; their effective contexts carry the
        # resolved route with its full (all prefixes applied) path, nested includes
        # flattened. Older versions flatten into app.routes and hit the branch above.
        contexts = getattr(route, "effective_route_contexts", None)

        if contexts is not None:
            for context in contexts():
                _collect(getattr(context, "starlette_route", None))

    for route in getattr(app, "routes", ()):
        _collect(route)

    for path in sorted(allowlisted):
        endpoints = websocket_routes.get(path, [])

        if not endpoints:
            raise exc.configuration(
                f"allowed_websocket_paths lists {path!r}, but no websocket route is "
                "registered at that exact path. If the governed route sits under a "
                "router prefix, allowlist the full mounted path; a path inside a "
                "mounted sub-application cannot be verified and must not be allowlisted."
            )

        if len(endpoints) > 1:
            raise exc.configuration(
                f"allowed_websocket_paths lists {path!r}, but {len(endpoints)} websocket "
                "routes are registered there — one allowlisted path must serve exactly "
                "one governed endpoint."
            )

        if not getattr(endpoints[0], GOVERNED_WEBSOCKET_ATTR, False):
            raise exc.configuration(
                f"allowed_websocket_paths lists {path!r}, but the websocket route there "
                "is not a governed realtime route — allowlisting it would serve "
                "unauthenticated, tenant-free websocket traffic. Attach it with "
                "attach_realtime_ws_route, or self-manage it under allow_raw_websockets."
            )


async def refuse_raw_websocket(scope: Scope, receive: Receive, send: Send) -> None:
    """Refuse a raw ``websocket`` scope at the ASGI level.

    Governed middlewares resolve identity, tenancy, and the invocation envelope
    for HTTP scopes only — a raw websocket route mounted on the same app would
    silently run without any of it. Instead of passing such scopes through
    unauthenticated, the middlewares fail closed and close the handshake with a
    policy violation (servers surface this as a 403-rejected upgrade). Apps that
    deliberately self-manage websocket routes opt out per middleware with
    ``allow_raw_websockets=True`` and own identity, tenancy, and error shaping
    on every websocket route themselves.
    """

    # Sent before ``websocket.accept``: ASGI servers reject the upgrade handshake.
    await send(
        {
            "type": "websocket.close",
            "code": WS_POLICY_VIOLATION,
            "reason": "raw websocket ingress is disabled (allow_raw_websockets=False)",
        }
    )
