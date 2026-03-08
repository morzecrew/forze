from typing import TYPE_CHECKING, Protocol, TypeVar, runtime_checkable

from pydantic import BaseModel

from ..deps import DepKey
from .ports import StreamGroupPort, StreamReadPort, StreamWritePort
from .specs import StreamSpec

if TYPE_CHECKING:
    from forze.application.execution.context import ExecutionContext

# ----------------------- #

M = TypeVar("M", bound=BaseModel)

# ....................... #


@runtime_checkable
class StreamReadDepPort(Protocol):
    def __call__(
        self,
        context: "ExecutionContext",
        spec: StreamSpec[M],
    ) -> StreamReadPort[M]: ...


# ....................... #


@runtime_checkable
class StreamWriteDepPort(Protocol):
    def __call__(
        self,
        context: "ExecutionContext",
        spec: StreamSpec[M],
    ) -> StreamWritePort[M]: ...


# ....................... #


@runtime_checkable
class StreamGroupDepPort(Protocol):
    def __call__(
        self,
        context: "ExecutionContext",
        spec: StreamSpec[M],
    ) -> StreamGroupPort[M]: ...


# ....................... #

StreamReadDepKey = DepKey[StreamReadDepPort]("stream_read")
StreamWriteDepKey = DepKey[StreamWriteDepPort]("stream_write")
StreamGroupDepKey = DepKey[StreamGroupDepPort]("stream_group")
