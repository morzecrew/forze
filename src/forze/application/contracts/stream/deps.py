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
    """Factory protocol for building :class:`StreamReadPort` instances."""

    def __call__(
        self,
        context: "ExecutionContext",
        spec: StreamSpec[M],
    ) -> StreamReadPort[M]:
        """Build a stream read port bound to the given context and spec."""
        ...


# ....................... #


@runtime_checkable
class StreamWriteDepPort(Protocol):
    """Factory protocol for building :class:`StreamWritePort` instances."""

    def __call__(
        self,
        context: "ExecutionContext",
        spec: StreamSpec[M],
    ) -> StreamWritePort[M]:
        """Build a stream write port bound to the given context and spec."""
        ...


# ....................... #


@runtime_checkable
class StreamGroupDepPort(Protocol):
    """Factory protocol for building :class:`StreamGroupPort` instances."""

    def __call__(
        self,
        context: "ExecutionContext",
        spec: StreamSpec[M],
    ) -> StreamGroupPort[M]:
        """Build a stream group port bound to the given context and spec."""
        ...


# ....................... #

StreamReadDepKey = DepKey[StreamReadDepPort]("stream_read")
"""Key used to register the :class:`StreamReadDepPort` implementation."""

StreamWriteDepKey = DepKey[StreamWriteDepPort]("stream_write")
"""Key used to register the :class:`StreamWriteDepPort` implementation."""

StreamGroupDepKey = DepKey[StreamGroupDepPort]("stream_group")
"""Key used to register the :class:`StreamGroupDepPort` implementation."""
