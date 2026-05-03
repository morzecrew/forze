"""Structural protocol for RabbitMQ clients (single DSN or tenant-routed)."""

from datetime import datetime, timedelta
from typing import AsyncContextManager, AsyncIterator, Awaitable, Protocol, Sequence

from aio_pika.abc import AbstractChannel

from .types import RabbitMQQueueMessage

# ----------------------- #


class RabbitMQClientPort(Protocol):
    """Operations implemented by :class:`RabbitMQClient` and routed variants."""

    def close(self) -> Awaitable[None]: ...  # pragma: no cover

    def health(self) -> Awaitable[tuple[str, bool]]: ...  # pragma: no cover

    def channel(self) -> AsyncContextManager[AbstractChannel]: ...  # pragma: no cover

    def enqueue(
        self,
        queue: str,
        body: bytes,
        *,
        type: str | None = None,
        key: str | None = None,
        enqueued_at: datetime | None = None,
        message_id: str | None = None,
    ) -> Awaitable[str]: ...  # pragma: no cover

    def enqueue_many(
        self,
        queue: str,
        bodies: Sequence[bytes],
        *,
        type: str | None = None,
        key: str | None = None,
        enqueued_at: datetime | None = None,
        message_ids: Sequence[str] | None = None,
    ) -> Awaitable[list[str]]: ...  # pragma: no cover

    def receive(
        self,
        queue: str,
        *,
        limit: int | None = None,
        timeout: timedelta | None = None,
    ) -> Awaitable[list[RabbitMQQueueMessage]]: ...  # pragma: no cover

    def consume(
        self,
        queue: str,
        *,
        timeout: timedelta | None = None,
    ) -> AsyncIterator[RabbitMQQueueMessage]:
        """Async iterator of queue messages (async generator on implementations)."""
        ...  # pragma: no cover

    def ack(
        self,
        queue: str,
        ids: Sequence[str],
    ) -> Awaitable[int]: ...  # pragma: no cover

    def nack(
        self,
        queue: str,
        ids: Sequence[str],
        *,
        requeue: bool = True,
    ) -> Awaitable[int]: ...  # pragma: no cover
