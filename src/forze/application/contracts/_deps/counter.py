from typing import TYPE_CHECKING, Protocol, final, runtime_checkable

import attrs

from .._ports.counter import CounterPort
from .base import DepKey, DepRouter

if TYPE_CHECKING:
    from forze.application.execution.context import ExecutionContext

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

CounterDepKey = DepKey[CounterDepPort]("counter")
"""Key used to register the :class:`CounterDepPort` implementation."""


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class CounterDepRouter(DepRouter[str, CounterDepPort], CounterDepPort):
    dep_key = CounterDepKey

    # ....................... #

    def __call__(self, context: "ExecutionContext", namespace: str) -> CounterPort:
        route = self._select(namespace)

        return route(context, namespace)
