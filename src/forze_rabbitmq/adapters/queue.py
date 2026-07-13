from forze_rabbitmq._compat import require_rabbitmq

require_rabbitmq()

# ....................... #

from collections.abc import AsyncGenerator, Mapping, Sequence
from datetime import datetime, timedelta
from typing import ClassVar, final

import attrs
from pydantic import BaseModel

from forze.application.contracts.queue import (
    QueueCommandPort,
    QueueMessage,
    QueueQueryPort,
)
from forze.application.integrations.queue import ScopedQueueNamingMixin

from ..kernel.client import RabbitMQClientPort
from ._logger import logger
from .codecs import RabbitMQQueueCodec

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class RabbitMQQueueAdapter[M: BaseModel](
    QueueQueryPort[M],
    QueueCommandPort[M],
    ScopedQueueNamingMixin,
):
    """RabbitMQ queue adapter.

    Poison messages: a payload that fails codec decoding in :meth:`receive`
    or :meth:`consume` is rejected with ``nack(requeue=False)`` (dead-letter
    when the queue has a DLX configured, dropped otherwise — terminal
    disposition is broker-specific per the queue port contract), logged by
    message id only (never payload contents), and *skipped*: the remaining
    batch is still returned and a continuous consumer keeps consuming.
    """

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
        headers: Mapping[str, str] | None = None,
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
            headers=headers,
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
        headers: Mapping[str, str] | None = None,
        message_headers: Sequence[Mapping[str, str]] | None = None,
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
            headers=headers,
            message_headers=message_headers,
        )

    # ....................... #

    async def __nack_poison(self, physical_queue: str, message_id: str) -> None:
        """Reject an undecodable message without requeue (dead-letter or drop).

        ``requeue=False`` sends the message to the queue's DLX when one is
        configured (or drops it otherwise) — requeueing a poison message
        would only redeliver it forever. Best-effort: a failed nack leaves
        the message pending on the client (returned to the broker on close).
        """

        try:
            await self.client.nack(physical_queue, [message_id], requeue=False)
        except Exception:
            logger.error(
                "RabbitMQ queue %s: failed to nack undecodable message %s; "
                "it stays pending until this client closes",
                physical_queue,
                message_id,
                exc_info=True,
            )

    # ....................... #

    async def receive(
        self,
        queue: str,
        *,
        limit: int | None = None,
        timeout: timedelta | None = None,
    ) -> list[QueueMessage[M]]:
        """Receive up to ``limit`` decoded messages.

        Undecodable (poison) entries are nacked away with ``requeue=False``
        and excluded from the result; the decodable remainder of the batch
        is still returned. See the class docstring for the full contract.
        """
        physical_queue = await self.__queue_name(queue)
        raw = await self.client.receive(physical_queue, limit=limit, timeout=timeout)

        decoded: list[QueueMessage[M]] = []

        for msg in raw:
            try:
                decoded.append(self.codec.decode(queue, msg))
            except Exception:
                # Poison entries are nacked away (requeue=False) and the
                # successfully decoded remainder is still returned: one bad
                # payload must not fail or wedge the whole batch.
                logger.error(
                    "RabbitMQ queue %s: failed to decode message %s; rejecting without requeue",
                    physical_queue,
                    msg.id,
                    exc_info=True,
                )
                await self.__nack_poison(physical_queue, msg.id)

        return decoded

    # ....................... #

    async def consume(
        self,
        queue: str,
        *,
        timeout: timedelta | None = None,
    ) -> AsyncGenerator[QueueMessage[M]]:
        """Yield decoded messages continuously from ``queue``.

        A decode failure must not kill the consumer: undecodable (poison)
        messages are nacked away with ``requeue=False`` and the stream keeps
        yielding subsequent messages. See the class docstring for the full
        contract.
        """
        physical_queue = await self.__queue_name(queue)

        async for msg in self.client.consume(physical_queue, timeout=timeout):
            try:
                decoded = self.codec.decode(queue, msg)
            except Exception:
                # A poison message is nacked away (requeue=False) and the
                # loop keeps consuming: a single undecodable payload must
                # not crash (and thereby wedge) the consumer stream. Idle
                # timeout semantics are untouched — the client iterator
                # already counted this delivery as activity.
                logger.error(
                    "RabbitMQ queue %s: failed to decode message %s; rejecting without requeue",
                    physical_queue,
                    msg.id,
                    exc_info=True,
                )
                await self.__nack_poison(physical_queue, msg.id)
                continue

            yield decoded

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
