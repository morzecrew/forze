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
    def __call__(
        self,
        context: "ExecutionContext",
        spec: QueueSpec[M],
    ) -> QueueReadPort[M]: ...


# ....................... #


@runtime_checkable
class QueueWriteDepPort(Protocol):
    def __call__(
        self,
        context: "ExecutionContext",
        spec: QueueSpec[M],
    ) -> QueueWritePort[M]: ...


# ....................... #

QueueReadDepKey = DepKey[QueueReadDepPort]("queue_read")
QueueWriteDepKey = DepKey[QueueWriteDepPort]("queue_write")
