from forze_sqs._compat import require_sqs

require_sqs()

# ....................... #

from datetime import datetime, timedelta
from typing import AsyncGenerator, ClassVar, Sequence, final

import attrs
from pydantic import BaseModel

from forze.application.contracts.queue import (
    QueueCommandPort,
    QueueMessage,
    QueueQueryPort,
)
from forze.application.integrations.queue import ScopedQueueNamingMixin
from forze.base.exceptions import exc

from ..kernel.client import SQSClientPort
from .codecs import SQSQueueCodec

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class SQSQueueAdapter[M: BaseModel](
    QueueQueryPort[M],
    QueueCommandPort[M],
    ScopedQueueNamingMixin,
):
    """SQS queue adapter."""

    client: SQSClientPort
    """SQS client instance."""

    codec: SQSQueueCodec[M]
    """SQS queue codec instance."""

    queue_name_separator: ClassVar[str] = "-"
    queue_backend_label: ClassVar[str] = "SQS queue"

    # ....................... #

    @staticmethod
    def __is_queue_url(queue: str) -> bool:
        return queue.startswith("https://") or queue.startswith("http://")

    # ....................... #

    async def __queue_name(self, queue: str) -> str:
        if self.__is_queue_url(queue):
            # An absolute queue URL skips namespace + tenant prefixing entirely.
            # On a tenant-aware adapter that is a tenant-isolation bypass, so reject it.
            if self.tenant_aware:
                raise exc.precondition(
                    "Absolute SQS queue URLs are not allowed on a tenant-aware "
                    "adapter: they bypass tenant prefixing and isolation.",
                )

            return queue

        return await self._scoped_queue_name(queue)

    # ....................... #

    async def enqueue(
        self,
        queue: str,
        payload: M,
        *,
        type: str | None = None,
        key: str | None = None,
        enqueued_at: datetime | None = None,
        delay: timedelta | None = None,
        not_before: datetime | None = None,
    ) -> str:
        physical_queue = await self.__queue_name(queue)
        body = self.codec.encode(payload)

        async with self.client.client():
            return await self.client.enqueue(
                physical_queue,
                body,
                type=type,
                key=key,
                enqueued_at=enqueued_at,
                delay=delay,
                not_before=not_before,
            )

    # ....................... #

    async def enqueue_many(
        self,
        queue: str,
        payloads: Sequence[M],
        *,
        type: str | None = None,
        key: str | None = None,
        enqueued_at: datetime | None = None,
        delay: timedelta | None = None,
        not_before: datetime | None = None,
    ) -> list[str]:
        if not payloads:
            return []

        physical_queue = await self.__queue_name(queue)
        bodies = [self.codec.encode(payload) for payload in payloads]

        async with self.client.client():
            return await self.client.enqueue_many(
                physical_queue,
                bodies,
                type=type,
                key=key,
                enqueued_at=enqueued_at,
                delay=delay,
                not_before=not_before,
            )

    # ....................... #

    async def receive(
        self,
        queue: str,
        *,
        limit: int | None = None,
        timeout: timedelta | None = None,
    ) -> list[QueueMessage[M]]:
        physical_queue = await self.__queue_name(queue)

        async with self.client.client():
            raw = await self.client.receive(
                physical_queue,
                limit=limit,
                timeout=timeout,
            )

        return [self.codec.decode(queue, msg) for msg in raw]

    # ....................... #

    async def consume(
        self,
        queue: str,
        *,
        timeout: timedelta | None = None,
    ) -> AsyncGenerator[QueueMessage[M]]:
        physical_queue = await self.__queue_name(queue)

        async with self.client.client():
            async for msg in self.client.consume(physical_queue, timeout=timeout):
                yield self.codec.decode(queue, msg)

    # ....................... #

    async def ack(self, queue: str, ids: Sequence[str]) -> int:
        physical_queue = await self.__queue_name(queue)

        async with self.client.client():
            return await self.client.ack(physical_queue, ids)

    # ....................... #

    async def nack(
        self,
        queue: str,
        ids: Sequence[str],
        *,
        requeue: bool = True,
    ) -> int:
        physical_queue = await self.__queue_name(queue)

        async with self.client.client():
            return await self.client.nack(physical_queue, ids, requeue=requeue)
