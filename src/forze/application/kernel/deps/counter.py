from typing import TYPE_CHECKING, Callable, Protocol, final, runtime_checkable

import attrs

from ..ports import CounterPort
from .base import DepKey, RoutingKey

if TYPE_CHECKING:
    from ..context import ExecutionContext

# ----------------------- #


@runtime_checkable
class CounterDepPort(Protocol):
    """Factory protocol for building :class:`CounterPort` instances."""

    def __call__(
        self,
        context: "ExecutionContext",
        namespace: str,
    ) -> CounterPort:
        """Build a counter port bound to the given context and namespace."""
        ...


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class CounterDepRouter(CounterDepPort):
    selector: Callable[[str], RoutingKey]
    routes: dict[RoutingKey, CounterDepPort]
    default: CounterDepPort

    # ....................... #

    def __call__(self, context: "ExecutionContext", namespace: str) -> CounterPort:
        sel = self.selector(namespace)
        route = self.routes.get(sel, self.default)
        return route(context, namespace)


# ....................... #

CounterDepKey: DepKey[CounterDepPort] = DepKey("counter")
"""Key used to register the :class:`CounterDepPort` implementation."""
