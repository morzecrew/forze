"""Structural protocol for SQS clients (single endpoint or tenant-routed)."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import AsyncContextManager, AsyncIterator, Protocol, Sequence

from types_aiobotocore_sqs.client import SQSClient as AsyncSQSClient

from .types import SQSQueueMessage

# ----------------------- #


class SQSClientPort(Protocol):
    """Operations implemented by :class:`SQSClient` and routed variants."""

    async def close(self) -> None:
        ...  # pragma: no cover

    def client(self) -> AsyncContextManager[AsyncSQSClient]:
        ...  # pragma: no cover

    async def health(self) -> tuple[str, bool]:
        ...  # pragma: no cover

    async def create_queue(
        self,
        queue: str,
        *,
        attributes: dict[str, str] | None = None,
    ) -> str:
        ...  # pragma: no cover

    async def queue_url(self, queue: str) -> str:
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
    ) -> list[SQSQueueMessage]:
        ...  # pragma: no cover

    def consume(
        self,
        queue: str,
        *,
        timeout: timedelta | None = None,
    ) -> AsyncIterator[SQSQueueMessage]:
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
