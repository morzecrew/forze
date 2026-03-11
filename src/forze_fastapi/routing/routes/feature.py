"""Composable route feature protocol and composition engine.

Provides a :class:`RouteFeature` protocol for defining route-level
behaviors (idempotency, ETag, tracing, etc.) that can be stacked on a
single :class:`~fastapi.routing.APIRoute` without subclass conflicts.
"""

from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from collections.abc import Awaitable, Callable, Sequence
from typing import Any, Protocol, runtime_checkable

from fastapi import Request, Response
from fastapi.params import Depends
from fastapi.routing import APIRoute

# ----------------------- #

RouteHandler = Callable[[Request], Awaitable[Response]]
"""Async callable that processes an HTTP request and returns a response."""

# ....................... #


@runtime_checkable
class RouteFeature(Protocol):
    """Route-level behavior wrapper composable via :func:`compose_route_class`.

    Each feature wraps the route handler with its own logic (e.g. caching,
    header injection, auditing). Features are applied in declaration order:
    the first feature in the sequence is the outermost wrapper.
    """

    def wrap(self, handler: RouteHandler) -> RouteHandler:
        """Wrap *handler* with this feature's request/response logic.

        :param handler: The next handler in the chain.
        :returns: A new handler that includes this feature's behavior.
        """
        ...

    @property
    def extra_dependencies(self) -> Sequence[Depends]:
        """Additional FastAPI dependencies injected when this feature is active."""
        ...


# ....................... #


def compose_route_class(
    *features: RouteFeature,
    base: type[APIRoute] = APIRoute,
) -> type[APIRoute]:
    """Build an :class:`~fastapi.routing.APIRoute` subclass that chains *features*.

    Features are applied so that the first element in *features* is the
    outermost wrapper (sees the request first and the response last).

    :param features: One or more :class:`RouteFeature` instances to compose.
    :param base: Base route class used for the generated subclass.
    :returns: A new :class:`APIRoute` subclass with all features applied.
    """

    captured = tuple(features)

    class _ComposedRoute(base):  # type: ignore[misc, valid-type]

        def get_route_handler(self) -> RouteHandler:  # type: ignore[override]
            handler: RouteHandler = super().get_route_handler()  # type: ignore[assignment]

            for feature in reversed(captured):
                handler = feature.wrap(handler)

            return handler

        def __init_subclass__(cls, **kwargs: Any) -> None:
            super().__init_subclass__(**kwargs)

    return _ComposedRoute
