"""Structural protocol for RabbitMQ clients (single DSN or tenant-routed)."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import AsyncContextManager, AsyncIterator, Protocol, Sequence

from aio_pika.abc import AbstractChannel

from .types import RabbitMQQueueMessage

# ----------------------- #


class RabbitMQClientPort(Protocol):
    """Operations implemented by :class:`RabbitMQClient` and routed variants."""

    async def close(self) -> None:
        ...  # pragma: no cover

    async def health(self) -> tuple[str, bool]:
        ...  # pragma: no cover

    def channel(self) -> AsyncContextManager[AbstractChannel]:
        ...  # pragma: no cover

    async def enqueue(
        self,
        queue: str,
        body: bytes,
        *,
        type: str | None = None,
        key: str | None = None,
        enqueued_at: datetime | None = None,
        message_id: str | None = None,
    ) -> str:
        ...  # pragma: no cover

    async def enqueue_many(
        self,
        queue: str,
        bodies: Sequence[bytes],
        *,
        type: str | None = None,
        key: str | None = None,
        enqueued_at: datetime | None = None,
        message_ids: Sequence[str] | None = None,
    ) -> list[str]:
        ...  # pragma: no cover

    async def receive(
        self,
        queue: str,
        *,
        limit: int | None = None,
        timeout: timedelta | None = None,
    ) -> list[RabbitMQQueueMessage]:
        ...  # pragma: no cover

    def consume(
        self,
        queue: str,
        *,
        timeout: timedelta | None = None,
    ) -> AsyncIterator[RabbitMQQueueMessage]:
        """Async iterator of queue messages (async generator on implementations)."""
        ...  # pragma: no cover

    async def ack(self, queue: str, ids: Sequence[str]) -> int:
        ...  # pragma: no cover

    async def nack(
        self,
        queue: str,
        ids: Sequence[str],
        *,
        requeue: bool = True,
    ) -> int:
        ...  # pragma: no cover
