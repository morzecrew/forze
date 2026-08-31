from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from collections.abc import Iterator
from typing import Any

from fastapi.routing import iter_route_contexts

# ----------------------- #


def iter_effective_routes(app: Any) -> Iterator[Any]:
    """Every route an app serves, with all router prefixes already applied.

    The startup checks compare configured *paths* against what the app actually
    routes, so they need the mounted path rather than the route-local one.
    ``iter_route_contexts`` flattens nested router includes; the public path
    accessors are empty for websocket routes, so the effective (all-prefixes-applied)
    route is taken from the include context when there is one. A flat top-level route
    has no include context and its own path is already the full one.
    """

    for context in iter_route_contexts(list(getattr(app, "routes", ()))):
        effective = getattr(getattr(context, "_route_context", None), "starlette_route", None)

        yield effective if effective is not None else context.route
