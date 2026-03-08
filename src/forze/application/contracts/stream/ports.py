from datetime import datetime, timedelta
from typing import (
    AsyncIterator,
    Awaitable,
    Optional,
    Protocol,
    Sequence,
    runtime_checkable,
)

from pydantic import BaseModel

from .types import StreamMessage

# ----------------------- #


@runtime_checkable
class StreamReadPort[M: BaseModel](Protocol):
    def read(
        self,
        stream_mapping: dict[str, str],
        *,
        limit: Optional[int] = None,
        timeout: Optional[timedelta] = None,
    ) -> Awaitable[list[StreamMessage[M]]]: ...

    # ....................... #

    def tail(
        self,
        stream_mapping: dict[str, str],
        *,
        timeout: Optional[timedelta] = None,
    ) -> AsyncIterator[StreamMessage[M]]: ...


# ....................... #


@runtime_checkable
class StreamGroupPort[M: BaseModel](Protocol):
    def read(
        self,
        group: str,
        consumer: str,
        stream_mapping: dict[str, str],
        *,
        limit: Optional[int] = None,
        timeout: Optional[timedelta] = None,
    ) -> Awaitable[list[StreamMessage[M]]]: ...

    # ....................... #

    def tail(
        self,
        group: str,
        consumer: str,
        stream_mapping: dict[str, str],
        *,
        timeout: Optional[timedelta] = None,
    ) -> AsyncIterator[StreamMessage[M]]: ...

    # ....................... #

    def ack(self, group: str, stream: str, ids: Sequence[str]) -> Awaitable[int]: ...


# ....................... #


@runtime_checkable
class StreamWritePort[M: BaseModel](Protocol):
    def append(
        self,
        stream: str,
        payload: M,
        *,
        type: Optional[str] = None,
        key: Optional[str] = None,
        timestamp: Optional[datetime] = None,
    ) -> Awaitable[str]: ...
