from forze_sqs._compat import require_sqs

require_sqs()

# ....................... #

from datetime import datetime, timedelta
from typing import AsyncIterator, Optional, Sequence, final

import attrs
from pydantic import BaseModel

from forze.application.contracts.queue import (
    QueueMessage,
    QueueReadPort,
    QueueWritePort,
)
from forze.base.errors import CoreError

from ..kernel.platform import SQSClient, SQSQueueMessage

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class SQSQueueCodec[M: BaseModel]:
    model: type[M]

    # ....................... #

    def encode(self, payload: M) -> bytes:
        return payload.model_dump_json().encode("utf-8")

    # ....................... #

    def decode(self, queue: str, raw: SQSQueueMessage) -> QueueMessage[M]:
        body = raw["body"]

        if not isinstance(body, (bytes, bytearray)):  # pyright: ignore[reportUnnecessaryIsInstance]
            raise CoreError(f"SQS queue message '{raw['id']}' has invalid payload")

        return QueueMessage(
            queue=queue,
            id=raw["id"],
            payload=self.model.model_validate_json(body),
            type=raw.get("type"),
            enqueued_at=raw.get("enqueued_at"),
            key=raw.get("key"),
        )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class SQSQueueAdapter[M: BaseModel](QueueReadPort[M], QueueWritePort[M]):
    client: SQSClient
    codec: SQSQueueCodec[M]
    namespace: str = ""

    # ....................... #

    @staticmethod
    def __is_queue_url(queue: str) -> bool:
        return queue.startswith("https://") or queue.startswith("http://")

    # ....................... #

    def __queue_name(self, queue: str) -> str:
        if self.__is_queue_url(queue):
            return queue

        if not self.namespace:
            return queue

        return f"{self.namespace}-{queue}"

    # ....................... #

    async def enqueue(
        self,
        queue: str,
        payload: M,
        *,
        type: Optional[str] = None,
        key: Optional[str] = None,
        enqueued_at: Optional[datetime] = None,
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
        type: Optional[str] = None,
        key: Optional[str] = None,
        enqueued_at: Optional[datetime] = None,
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
        limit: Optional[int] = None,
        timeout: Optional[timedelta] = None,
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
        timeout: Optional[timedelta] = None,
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
