from forze_rabbitmq._compat import require_rabbitmq

require_rabbitmq()

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
from forze_contrib.tenancy import MultiTenancyMixin

from ..kernel.platform import RabbitMQClient
from .codecs import RabbitMQQueueCodec

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class RabbitMQQueueAdapter[M: BaseModel](
    QueueQueryPort[M],
    QueueCommandPort[M],
    MultiTenancyMixin,
):
    """RabbitMQ queue adapter."""

    client: RabbitMQClient
    """RabbitMQ client instance."""

    codec: RabbitMQQueueCodec[M]
    """RabbitMQ queue codec instance."""

    namespace: str | None = attrs.field(default=None)
    """RabbitMQ queue namespace."""

    # ....................... #

    def __queue_name(self, queue: str) -> str:
        tenant_id = self.require_tenant_if_aware()

        if tenant_id is not None:
            tenant_prefix = f"tenant:{tenant_id}"

        else:
            tenant_prefix = ""

        if self.namespace:
            namespaced_queue = f"{self.namespace}:{queue}"

        else:
            namespaced_queue = queue

        return f"{tenant_prefix}:{namespaced_queue}".lstrip(":")

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
        raw = await self.client.receive(physical_queue, limit=limit, timeout=timeout)

        return [self.codec.decode(queue, msg) for msg in raw]

    # ....................... #

    async def consume(
        self,
        queue: str,
        *,
        timeout: timedelta | None = None,
    ) -> AsyncIterator[QueueMessage[M]]:
        physical_queue = self.__queue_name(queue)

        async for msg in self.client.consume(physical_queue, timeout=timeout):
            yield self.codec.decode(queue, msg)

    # ....................... #

    async def ack(self, queue: str, ids: Sequence[str]) -> int:
        physical_queue = self.__queue_name(queue)
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
        return await self.client.nack(physical_queue, ids, requeue=requeue)
