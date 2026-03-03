"""Ports for append-only event stream backends."""

from typing import (
    AsyncIterator,
    Awaitable,
    Optional,
    Protocol,
    Sequence,
    runtime_checkable,
)

from pydantic import BaseModel

from forze.base.primitives import JsonDict

from .types import StreamEvent

# ----------------------- #


@runtime_checkable
class StreamPort[M: BaseModel](Protocol):
    """Contract for event streams used by the application kernel."""

    def publish(
        self,
        stream: str,
        payload: M | JsonDict,
        *,
        type: Optional[str] = None,
        key: Optional[str] = None,
        ts: Optional[int] = None,
        id: str = "*",
        maxlen: Optional[int] = None,
        approx: Optional[bool] = None,
    ) -> Awaitable[str]:
        """Append a new event to a stream and return its backend ID."""
        ...

    def read(
        self,
        streams: dict[str, str],
        *,
        count: Optional[int] = None,
        block_ms: Optional[int] = None,
    ) -> Awaitable[list[StreamEvent[M]]]:
        """Read events from one or more streams in a blocking or polling mode."""
        ...

    def subscribe(
        self,
        stream: str,
        *,
        start_id: str = "$",
        block_ms: int = 5000,
        count: int = 200,
    ) -> AsyncIterator[StreamEvent[M]]:
        """Subscribe to a stream and yield events as they arrive."""
        ...

    def trim(
        self,
        stream: str,
        *,
        maxlen: int,
        approx: bool = True,
        limit: Optional[int] = None,
    ) -> Awaitable[int]:
        """Trim a stream to at most ``maxlen`` entries and return removed count."""
        ...

    def delete(self, stream: str, ids: Sequence[str]) -> Awaitable[int]:
        """Delete individual events by ID and return the number removed."""
        ...

    def ensure_group(
        self,
        stream: str,
        group: str,
        *,
        start_id: str = "0-0",
        mkstream: bool = True,
        ignore_busy: bool = True,
    ) -> Awaitable[bool]:
        """Ensure a consumer group exists for ``stream`` and ``group``."""
        ...

    def read_group(
        self,
        stream: str,
        group: str,
        consumer: str,
        *,
        start_id: str = ">",
        block_ms: Optional[int] = None,
        count: Optional[int] = None,
        noack: bool = False,
    ) -> Awaitable[list[StreamEvent[M]]]:
        """Read events for a consumer in a group."""
        ...

    def subscribe_group(
        self,
        stream: str,
        group: str,
        consumer: str,
        *,
        start_id: str = ">",
        block_ms: int = 5000,
        count: int = 200,
    ) -> AsyncIterator[StreamEvent[M]]:
        """Subscribe to a consumer group and yield events as they arrive."""
        ...

    def ack(self, stream: str, group: str, ids: Sequence[str]) -> Awaitable[int]:
        """Acknowledge processing of events for a consumer group."""
        ...
