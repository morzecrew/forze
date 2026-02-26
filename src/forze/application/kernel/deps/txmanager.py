from typing import TYPE_CHECKING, Callable, Protocol, final, runtime_checkable

import attrs

from ..ports import TxManagerPort
from .base import DepKey, RoutingKey

if TYPE_CHECKING:
    from ..context import ExecutionContext

# ----------------------- #


@runtime_checkable
class TxManagerDepPort(Protocol):
    """Factory protocol for building :class:`TxManagerPort` instances."""

    def __call__(self, context: "ExecutionContext") -> TxManagerPort:
        """Build a transaction manager port bound to the given context."""
        ...


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class TxManagerDepRouter(TxManagerDepPort):
    selector: Callable[[], RoutingKey]
    routes: dict[RoutingKey, TxManagerDepPort]
    default: TxManagerDepPort

    # ....................... #

    def __call__(self, context: "ExecutionContext") -> TxManagerPort:
        sel = self.selector()
        route = self.routes.get(sel, self.default)

        return route(context)


# ....................... #

TxManagerDepKey: DepKey[TxManagerDepPort] = DepKey("tx_manager")
"""Key used to register the :class:`TxManagerDepPort` implementation."""
