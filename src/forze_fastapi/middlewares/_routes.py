from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from collections.abc import Iterator
from typing import Any

from fastapi.routing import iter_route_contexts

# ----------------------- #


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

    :returns: ``(effective path, route)`` pairs — the route being the object that
        actually serves it, so callers read ``endpoint`` and match types on it.
    """

    for context in iter_route_contexts(list(getattr(app, "routes", ()))):
        route_context = getattr(context, "_route_context", None)
        starlette_route = getattr(route_context, "starlette_route", None)

        if starlette_route is not None:
            yield starlette_route.path, starlette_route

            continue

        # An empty context path means the context carries no path for this route
        # kind, not that the route is mounted at the root.
        context_path = getattr(route_context, "path", "") or ""

        route: Any = context.route

        yield context_path or getattr(route, "path", ""), route
