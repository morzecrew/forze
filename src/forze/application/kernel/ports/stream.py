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
    stream: str
    id: str
    type: NotRequired[Optional[str]]
    timestamp: NotRequired[Optional[int]]
    key: NotRequired[Optional[str]]
    data: M


# ....................... #


@runtime_checkable
class StreamPort[M: BaseModel](Protocol):
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
    ) -> str: ...

    async def read(
        self,
        streams: dict[str, str],
        *,
        count: Optional[int] = None,
        block_ms: Optional[int] = None,
    ) -> list[StreamEvent[M]]: ...

    def subscribe(
        self,
        stream: str,
        *,
        start_id: str = "$",
        block_ms: int = 5000,
        count: int = 200,
    ) -> AsyncIterator[StreamEvent[M]]: ...

    async def trim(
        self,
        stream: str,
        *,
        maxlen: int,
        approx: bool = True,
        limit: Optional[int] = None,
    ) -> int: ...

    async def delete(self, stream: str, ids: Sequence[str]) -> int: ...

    async def ensure_group(
        self,
        stream: str,
        group: str,
        *,
        start_id: str = "0-0",
        mkstream: bool = True,
        ignore_busy: bool = True,
    ) -> bool: ...

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
    ) -> list[StreamEvent[M]]: ...

    async def subscribe_group(
        self,
        stream: str,
        group: str,
        consumer: str,
        *,
        start_id: str = ">",
        block_ms: int = 5000,
        count: int = 200,
    ) -> AsyncIterator[StreamEvent[M]]: ...

    async def ack(self, stream: str, group: str, ids: Sequence[str]) -> int: ...
