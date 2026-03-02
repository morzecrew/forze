from typing import TYPE_CHECKING, Protocol, final, runtime_checkable

import attrs

from ..deps import DepKey, DepRouter
from .ports import TxManagerPort

if TYPE_CHECKING:
    from forze.application.execution.context import ExecutionContext

# ----------------------- #


@runtime_checkable
class TxManagerDepPort(Protocol):
    """Factory protocol for building :class:`TxManagerPort` instances."""

    def __call__(self, context: "ExecutionContext") -> TxManagerPort:
        """Build a transaction manager port bound to the given context."""
        ...


# ....................... #

TxManagerDepKey = DepKey[TxManagerDepPort]("tx_manager")
"""Key used to register the :class:`TxManagerDepPort` implementation."""


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class TxManagerDepRouter(DepRouter[None, TxManagerDepPort], TxManagerDepPort):
    dep_key = TxManagerDepKey

    # ....................... #

    def __call__(self, context: "ExecutionContext") -> TxManagerPort:
        route = self._select(None)

        return route(context)
