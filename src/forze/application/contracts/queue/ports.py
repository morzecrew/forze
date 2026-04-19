from datetime import datetime, timedelta
from typing import (
    AsyncIterator,
    Awaitable,
    Protocol,
    Sequence,
    runtime_checkable,
)

from pydantic import BaseModel

from .types import QueueMessage

# ----------------------- #


@runtime_checkable
class QueueQueryPort[M: BaseModel](Protocol):
    """Contract for reading and acknowledging messages from a queue backend."""

    def receive(
        self,
        queue: str,  # noqa: F841
        *,
        limit: int | None = None,
        timeout: timedelta | None = None,  # noqa: F841
    ) -> Awaitable[list[QueueMessage[M]]]:
        """Fetch a batch of messages from *queue*."""
        ...  # pragma: no cover

    # ....................... #

    def consume(
        self,
        queue: str,  # noqa: F841
        *,
        timeout: timedelta | None = None,  # noqa: F841
    ) -> AsyncIterator[QueueMessage[M]]:
        """Yield messages continuously from *queue* until *timeout* elapses."""
        ...  # pragma: no cover

    # ....................... #

    def ack(self, queue: str, ids: Sequence[str]) -> Awaitable[int]:  # noqa: F841
        """Acknowledge processed messages, returning the count acknowledged."""
        ...  # pragma: no cover

    # ....................... #

    def nack(
        self,
        queue: str,
        ids: Sequence[str],
        *,
        requeue: bool = True,
    ) -> Awaitable[int]:
        """Negatively acknowledge messages, optionally requeuing them."""
        ...  # pragma: no cover


# ....................... #


@runtime_checkable
class QueueCommandPort[M: BaseModel](Protocol):
    """Contract for publishing messages to a queue backend."""

    def enqueue(
        self,
        queue: str,
        payload: M,
        *,
        type: str | None = None,
        key: str | None = None,
        enqueued_at: datetime | None = None,
    ) -> Awaitable[str]:
        """Enqueue a single message and return its identifier."""
        ...  # pragma: no cover

    # ....................... #

    def enqueue_many(
        self,
        queue: str,
        payloads: Sequence[M],
        *,
        type: str | None = None,
        key: str | None = None,
        enqueued_at: datetime | None = None,
    ) -> Awaitable[list[str]]:
        """Enqueue multiple messages and return their identifiers."""
        ...  # pragma: no cover
