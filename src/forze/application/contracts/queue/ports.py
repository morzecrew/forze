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

from .types import QueueMessage

# ----------------------- #


@runtime_checkable
class QueueReadPort[M: BaseModel](Protocol):
    def receive(
        self,
        queue: str,  # noqa: F841
        *,
        limit: Optional[int] = None,
        timeout: Optional[timedelta] = None,  # noqa: F841
    ) -> Awaitable[list[QueueMessage[M]]]: ...

    # ....................... #

    def consume(
        self,
        queue: str,  # noqa: F841
        *,
        timeout: Optional[timedelta] = None,  # noqa: F841
    ) -> AsyncIterator[QueueMessage[M]]: ...

    # ....................... #

    def ack(self, queue: str, ids: Sequence[str]) -> Awaitable[int]: ...  # noqa: F841

    # ....................... #

    def nack(
        self,
        queue: str,
        ids: Sequence[str],
        *,
        requeue: bool = True,
    ) -> Awaitable[int]: ...


# ....................... #


@runtime_checkable
class QueueWritePort[M: BaseModel](Protocol):
    def enqueue(
        self,
        queue: str,
        payload: M,
        *,
        type: Optional[str] = None,
        key: Optional[str] = None,
        enqueued_at: Optional[datetime] = None,
    ) -> Awaitable[str]: ...

    # ....................... #

    def enqueue_many(
        self,
        queue: str,
        payloads: Sequence[M],
        *,
        type: Optional[str] = None,
        key: Optional[str] = None,
        enqueued_at: Optional[datetime] = None,
    ) -> Awaitable[list[str]]: ...
