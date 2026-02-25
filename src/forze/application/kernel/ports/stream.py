"""Ports for append-only event stream backends."""

from typing import (
    AsyncIterator,
    NotRequired,
    Optional,
    Protocol,
    Sequence,
    TypedDict,
    runtime_checkable,
)

from pydantic import BaseModel

from forze.base.primitives import JsonDict

# ----------------------- #


class StreamEvent[M: BaseModel](TypedDict):
    """Event as read from or written to a stream backend."""

    stream: str
    """Logical stream name or topic."""

    id: str
    """Backend-specific identifier for the event (e.g. Redis stream ID)."""

    type: NotRequired[Optional[str]]
    """Optional event type or category."""

    timestamp: NotRequired[Optional[int]]
    """Optional timestamp associated with the event."""

    key: NotRequired[Optional[str]]
    """Optional partitioning key for the event."""

    data: M
    """Structured payload carried by the event."""


# ....................... #


@runtime_checkable
class StreamPort[M: BaseModel](Protocol):
    """Contract for event streams used by the application kernel."""

    async def publish(
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
    ) -> str:
        """Append a new event to a stream and return its backend ID."""
        ...

    async def read(
        self,
        streams: dict[str, str],
        *,
        count: Optional[int] = None,
        block_ms: Optional[int] = None,
    ) -> list[StreamEvent[M]]:
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

    async def trim(
        self,
        stream: str,
        *,
        maxlen: int,
        approx: bool = True,
        limit: Optional[int] = None,
    ) -> int:
        """Trim a stream to at most ``maxlen`` entries and return removed count."""
        ...

    async def delete(self, stream: str, ids: Sequence[str]) -> int:
        """Delete individual events by ID and return the number removed."""
        ...

    async def ensure_group(
        self,
        stream: str,
        group: str,
        *,
        start_id: str = "0-0",
        mkstream: bool = True,
        ignore_busy: bool = True,
    ) -> bool:
        """Ensure a consumer group exists for ``stream`` and ``group``."""
        ...

    async def read_group(
        self,
        stream: str,
        group: str,
        consumer: str,
        *,
        start_id: str = ">",
        block_ms: Optional[int] = None,
        count: Optional[int] = None,
        noack: bool = False,
    ) -> list[StreamEvent[M]]:
        """Read events for a consumer in a group."""
        ...

    async def subscribe_group(
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

    async def ack(self, stream: str, group: str, ids: Sequence[str]) -> int:
        """Acknowledge processing of events for a consumer group."""
        ...
