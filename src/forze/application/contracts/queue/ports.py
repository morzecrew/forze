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
    """Contract for reading and acknowledging messages from a queue backend."""

    def receive(
        self,
        queue: str,  # noqa: F841
        *,
        limit: Optional[int] = None,
        timeout: Optional[timedelta] = None,  # noqa: F841
    ) -> Awaitable[list[QueueMessage[M]]]:
        """Fetch a batch of messages from *queue*."""
        ...

    # ....................... #

    def consume(
        self,
        queue: str,  # noqa: F841
        *,
        timeout: Optional[timedelta] = None,  # noqa: F841
    ) -> AsyncIterator[QueueMessage[M]]:
        """Yield messages continuously from *queue* until *timeout* elapses."""
        ...

    # ....................... #

    def ack(self, queue: str, ids: Sequence[str]) -> Awaitable[int]:  # noqa: F841
        """Acknowledge processed messages, returning the count acknowledged."""
        ...

    # ....................... #

    def nack(
        self,
        queue: str,
        ids: Sequence[str],
        *,
        requeue: bool = True,
    ) -> Awaitable[int]:
        """Negatively acknowledge messages, optionally requeuing them."""
        ...


# ....................... #


@runtime_checkable
class QueueWritePort[M: BaseModel](Protocol):
    """Contract for publishing messages to a queue backend."""

    def enqueue(
        self,
        queue: str,
        payload: M,
        *,
        type: Optional[str] = None,
        key: Optional[str] = None,
        enqueued_at: Optional[datetime] = None,
    ) -> Awaitable[str]:
        """Enqueue a single message and return its identifier."""
        ...

    # ....................... #

    def enqueue_many(
        self,
        queue: str,
        payloads: Sequence[M],
        *,
        type: Optional[str] = None,
        key: Optional[str] = None,
        enqueued_at: Optional[datetime] = None,
    ) -> Awaitable[list[str]]:
        """Enqueue multiple messages and return their identifiers."""
        ...
