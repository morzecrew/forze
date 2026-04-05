from datetime import datetime, timedelta
from typing import (
    AsyncIterator,
    Awaitable,
    Protocol,
    Sequence,
    runtime_checkable,
)

from pydantic import BaseModel

from .types import StreamMessage

# ----------------------- #


@runtime_checkable
class StreamQueryPort[M: BaseModel](Protocol):
    """Contract for querying messages from one or more streams."""

    def read(
        self,
        stream_mapping: dict[str, str],
        *,
        limit: int | None = None,
        timeout: timedelta | None = None,
    ) -> Awaitable[list[StreamMessage[M]]]:
        """Read a batch of messages from the streams in *stream_mapping*."""
        ...

    # ....................... #

    def tail(
        self,
        stream_mapping: dict[str, str],
        *,
        timeout: timedelta | None = None,
    ) -> AsyncIterator[StreamMessage[M]]:
        """Continuously yield new messages from the mapped streams."""
        ...


# ....................... #


@runtime_checkable
class StreamGroupQueryPort[M: BaseModel](Protocol):
    """Contract for consumer-group-based stream reads and acknowledgments."""

    def read(
        self,
        group: str,
        consumer: str,
        stream_mapping: dict[str, str],
        *,
        limit: int | None = None,
        timeout: timedelta | None = None,
    ) -> Awaitable[list[StreamMessage[M]]]:
        """Read pending messages for *consumer* in *group*."""
        ...

    # ....................... #

    def tail(
        self,
        group: str,
        consumer: str,
        stream_mapping: dict[str, str],
        *,
        timeout: timedelta | None = None,
    ) -> AsyncIterator[StreamMessage[M]]:
        """Continuously yield new messages for *consumer* in *group*."""
        ...

    # ....................... #

    def ack(self, group: str, stream: str, ids: Sequence[str]) -> Awaitable[int]:
        """Acknowledge processed messages within *group*."""
        ...


# ....................... #


@runtime_checkable
class StreamCommandPort[M: BaseModel](Protocol):
    """Contract for appending messages to a stream backend."""

    def append(
        self,
        stream: str,
        payload: M,
        *,
        type: str | None = None,
        key: str | None = None,
        timestamp: datetime | None = None,
    ) -> Awaitable[str]:
        """Append a message to *stream* and return its identifier."""
        ...
