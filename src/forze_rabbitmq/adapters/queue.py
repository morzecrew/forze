from forze_rabbitmq._compat import require_rabbitmq

require_rabbitmq()

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

from ..kernel.client import RabbitMQClientPort
from .codecs import RabbitMQQueueCodec

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class RabbitMQQueueAdapter[M: BaseModel](
    QueueQueryPort[M],
    QueueCommandPort[M],
    ScopedQueueNamingMixin,
):
    """RabbitMQ queue adapter."""

    client: RabbitMQClientPort
    """RabbitMQ client instance."""

    codec: RabbitMQQueueCodec[M]
    """RabbitMQ queue codec instance."""

    delayed_delivery: bool = False
    """Whether delayed enqueue uses the DLX delay-queue topology."""

    queue_name_separator: ClassVar[str] = ":"
    queue_backend_label: ClassVar[str] = "RabbitMQ queue"

    # ....................... #

    async def __queue_name(self, queue: str) -> str:
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

        return await self.client.enqueue(
            physical_queue,
            body,
            type=type,
            key=key,
            enqueued_at=enqueued_at,
            delay=delay,
            not_before=not_before,
            delayed_delivery=self.delayed_delivery,
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

        return await self.client.enqueue_many(
            physical_queue,
            bodies,
            type=type,
            key=key,
            enqueued_at=enqueued_at,
            delay=delay,
            not_before=not_before,
            delayed_delivery=self.delayed_delivery,
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
        raw = await self.client.receive(physical_queue, limit=limit, timeout=timeout)

        return [self.codec.decode(queue, msg) for msg in raw]

    # ....................... #

    async def consume(
        self,
        queue: str,
        *,
        timeout: timedelta | None = None,
    ) -> AsyncGenerator[QueueMessage[M]]:
        physical_queue = await self.__queue_name(queue)

        async for msg in self.client.consume(physical_queue, timeout=timeout):
            yield self.codec.decode(queue, msg)

    # ....................... #

    async def ack(self, queue: str, ids: Sequence[str]) -> int:
        physical_queue = await self.__queue_name(queue)
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
        return await self.client.nack(physical_queue, ids, requeue=requeue)
