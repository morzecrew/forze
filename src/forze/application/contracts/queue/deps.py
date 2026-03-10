from typing import TYPE_CHECKING, Protocol, TypeVar, runtime_checkable

from pydantic import BaseModel

from ..deps import DepKey
from .ports import QueueReadPort, QueueWritePort
from .specs import QueueSpec

if TYPE_CHECKING:
    from forze.application.execution.context import ExecutionContext

# ----------------------- #

M = TypeVar("M", bound=BaseModel)

# ....................... #


@runtime_checkable
class QueueReadDepPort(Protocol):
    """Factory protocol for building :class:`QueueReadPort` instances."""

    def __call__(
        self,
        context: "ExecutionContext",
        spec: QueueSpec[M],
    ) -> QueueReadPort[M]:
        """Build a queue read port bound to the given context and spec."""
        ...


# ....................... #


@runtime_checkable
class QueueWriteDepPort(Protocol):
    """Factory protocol for building :class:`QueueWritePort` instances."""

    def __call__(
        self,
        context: "ExecutionContext",
        spec: QueueSpec[M],
    ) -> QueueWritePort[M]:
        """Build a queue write port bound to the given context and spec."""
        ...


# ....................... #

QueueReadDepKey = DepKey[QueueReadDepPort]("queue_read")
"""Key used to register the :class:`QueueReadDepPort` implementation."""

QueueWriteDepKey = DepKey[QueueWriteDepPort]("queue_write")
"""Key used to register the :class:`QueueWriteDepPort` implementation."""
