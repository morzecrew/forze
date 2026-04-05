"""Transaction manager dependency keys and routers."""

from typing import TYPE_CHECKING, Protocol, runtime_checkable

from ..base import DepKey
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
