from forze_sqs._compat import require_sqs

require_sqs()

# ....................... #

from datetime import datetime, timedelta
from typing import AsyncIterator, Sequence, final

import attrs
from pydantic import BaseModel

from forze.application.contracts.queue import (
    QueueCommandPort,
    QueueMessage,
    QueueQueryPort,
)
from forze.infra.tenancy import MultiTenancyMixin

from ..kernel.platform import SQSClient
from .codecs import SQSQueueCodec

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class SQSQueueAdapter[M: BaseModel](
    QueueQueryPort[M],
    QueueCommandPort[M],
    MultiTenancyMixin,
):
    """SQS queue adapter."""

    client: SQSClient
    """SQS client instance."""

    codec: SQSQueueCodec[M]
    """SQS queue codec instance."""

    namespace: str | None = attrs.field(default=None)
    """SQS queue namespace."""

    # ....................... #

    @staticmethod
    def __is_queue_url(queue: str) -> bool:
        return queue.startswith("https://") or queue.startswith("http://")

    # ....................... #

    def __queue_name(self, queue: str) -> str:
        if self.__is_queue_url(queue):
            return queue

        tenant_id = self.require_tenant_if_aware()

        if tenant_id is not None:
            tenant_prefix = f"tenant-{tenant_id}"

        else:
            tenant_prefix = ""

        if self.namespace:
            namespaced_queue = f"{self.namespace}-{queue}"

        else:
            namespaced_queue = queue

        return f"{tenant_prefix}-{namespaced_queue}".lstrip("-")

    # ....................... #

    async def enqueue(
        self,
        queue: str,
        payload: M,
        *,
        type: str | None = None,
        key: str | None = None,
        enqueued_at: datetime | None = None,
    ) -> str:
        physical_queue = self.__queue_name(queue)
        body = self.codec.encode(payload)

        async with self.client.client():
            return await self.client.enqueue(
                physical_queue,
                body,
                type=type,
                key=key,
                enqueued_at=enqueued_at,
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
    ) -> list[str]:
        if not payloads:
            return []

        physical_queue = self.__queue_name(queue)
        bodies = [self.codec.encode(payload) for payload in payloads]

        async with self.client.client():
            return await self.client.enqueue_many(
                physical_queue,
                bodies,
                type=type,
                key=key,
                enqueued_at=enqueued_at,
            )

    # ....................... #

    async def receive(
        self,
        queue: str,
        *,
        limit: int | None = None,
        timeout: timedelta | None = None,
    ) -> list[QueueMessage[M]]:
        physical_queue = self.__queue_name(queue)
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
    ) -> AsyncIterator[QueueMessage[M]]:
        physical_queue = self.__queue_name(queue)

        async with self.client.client():
            async for msg in self.client.consume(physical_queue, timeout=timeout):
                yield self.codec.decode(queue, msg)

    # ....................... #

    async def ack(self, queue: str, ids: Sequence[str]) -> int:
        physical_queue = self.__queue_name(queue)

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
        physical_queue = self.__queue_name(queue)
        async with self.client.client():
            return await self.client.nack(physical_queue, ids, requeue=requeue)
