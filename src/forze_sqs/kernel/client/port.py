"""Structural protocol for SQS clients (single endpoint or tenant-routed)."""

from __future__ import annotations

from collections.abc import AsyncGenerator, Awaitable, Mapping, Sequence
from contextlib import AbstractAsyncContextManager
from datetime import datetime, timedelta
from typing import (
    TYPE_CHECKING,
    Protocol,
)

if TYPE_CHECKING:
    # Type-only stub package; kept off the runtime import path.
    from types_aiobotocore_sqs.client import SQSClient as AsyncSQSClient

from .constants import SQS_DEFAULT_MAX_BATCH_PAYLOAD_BYTES
from .types import SQSQueueMessage

# ----------------------- #


class SQSClientPort(Protocol):
    """Operations implemented by :class:`SQSClient` and routed variants."""

    def close(self) -> Awaitable[None]: ...  # pragma: no cover

    def client(self) -> AbstractAsyncContextManager[AsyncSQSClient]: ...  # pragma: no cover

    def health(self) -> Awaitable[tuple[str, bool]]: ...  # pragma: no cover

    def create_queue(
        self,
        queue: str,
        *,
        attributes: dict[str, str] | None = None,
    ) -> Awaitable[str]: ...  # pragma: no cover

    def queue_url(self, queue: str) -> Awaitable[str]: ...  # pragma: no cover

    def enqueue(
        self,
        queue: str,
        body: bytes,
        *,
        type: str | None = None,
        key: str | None = None,
        enqueued_at: datetime | None = None,
        message_id: str | None = None,
        delay: timedelta | None = None,
        not_before: datetime | None = None,
        headers: Mapping[str, str] | None = None,
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
        delay: timedelta | None = None,
        not_before: datetime | None = None,
        headers: Mapping[str, str] | None = None,
        message_headers: Sequence[Mapping[str, str]] | None = None,
        max_batch_payload_bytes: int = SQS_DEFAULT_MAX_BATCH_PAYLOAD_BYTES,
    ) -> Awaitable[list[str]]: ...  # pragma: no cover

    def receive(
        self,
        queue: str,
        *,
        limit: int | None = None,
        timeout: timedelta | None = None,
    ) -> Awaitable[list[SQSQueueMessage]]: ...  # pragma: no cover

    def consume(
        self,
        queue: str,
        *,
        timeout: timedelta | None = None,
    ) -> AsyncGenerator[SQSQueueMessage]: ...  # pragma: no cover

    def ack(self, queue: str, ids: Sequence[str]) -> Awaitable[int]: ...  # pragma: no cover

    def nack(
        self,
        queue: str,
        ids: Sequence[str],
        *,
        requeue: bool = True,
    ) -> Awaitable[int]: ...  # pragma: no cover
