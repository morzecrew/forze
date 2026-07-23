from forze_sqs._compat import require_sqs

require_sqs()

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
from forze.base.exceptions import exc

from ..kernel.client import SQS_DEFAULT_MAX_BATCH_PAYLOAD_BYTES, SQSClientPort
from ._logger import logger
from .codecs import SQSQueueCodec

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class SQSQueueAdapter[M: BaseModel](
    QueueQueryPort[M],
    QueueCommandPort[M],
    ScopedQueueNamingMixin,
):
    """SQS queue adapter.

    Poison messages: a payload that fails codec decoding in :meth:`receive`
    or :meth:`consume` is rejected with ``nack(requeue=False)`` (the message
    stays invisible until its visibility timeout lapses, so the queue's
    redrive policy eventually dead-letters it — terminal disposition is
    broker-specific per the queue port contract), logged by message id only
    (never payload contents), and *skipped*: the remaining batch is still
    returned and a continuous consumer keeps consuming.
    """

    client: SQSClientPort
    """SQS client instance."""

    codec: SQSQueueCodec[M]
    """SQS queue codec instance."""

    max_batch_payload_bytes: int = SQS_DEFAULT_MAX_BATCH_PAYLOAD_BYTES
    """Per-queue ``send_message_batch`` payload cap (from :class:`SQSQueueConfig`)."""

    queue_name_separator: ClassVar[str] = "-"
    queue_backend_label: ClassVar[str] = "SQS queue"

    # ....................... #

    @staticmethod
    def __is_queue_url(queue: str) -> bool:
        return queue.startswith(("https://", "http://"))

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
        headers: Mapping[str, str] | None = None,
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

        async with self.client.client():
            return await self.client.enqueue_many(
                physical_queue,
                bodies,
                type=type,
                key=key,
                enqueued_at=enqueued_at,
                delay=delay,
                not_before=not_before,
                headers=headers,
                message_headers=message_headers,
                max_batch_payload_bytes=self.max_batch_payload_bytes,
            )

    # ....................... #

    async def __nack_poison(self, physical_queue: str, message_id: str) -> None:
        """Reject an undecodable message without requeue (redrive/dead-letter).

        ``requeue=False`` leaves the message invisible until its visibility
        timeout lapses, so the queue's redrive policy counts the receive and
        eventually dead-letters it — requeueing a poison message would only
        redeliver it forever. Best-effort: a failed nack leaves the message
        invisible until its visibility timeout lapses anyway.
        """

        try:
            async with self.client.client():
                await self.client.nack(physical_queue, [message_id], requeue=False)
        except Exception:
            logger.error(
                "SQS queue %s: failed to nack undecodable message %s; "
                "it stays invisible until its visibility timeout lapses",
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

        async with self.client.client():
            raw = await self.client.receive(
                physical_queue,
                limit=limit,
                timeout=timeout,
            )

        decoded: list[QueueMessage[M]] = []

        for msg in raw:
            try:
                decoded.append(self.codec.decode(queue, msg))
            except Exception:
                # Poison entries are nacked away (requeue=False) and the
                # successfully decoded remainder is still returned: one bad
                # payload must not fail or wedge the whole batch.
                logger.error(
                    "SQS queue %s: failed to decode message %s; rejecting without requeue",
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

        async with self.client.client():
            async for msg in self.client.consume(physical_queue, timeout=timeout):
                try:
                    decoded = self.codec.decode(queue, msg)
                except Exception:
                    # A poison message is nacked away (requeue=False) and the
                    # loop keeps consuming: a single undecodable payload must
                    # not crash (and thereby wedge) the consumer stream.
                    logger.error(
                        "SQS queue %s: failed to decode message %s; rejecting without requeue",
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

        async with self.client.client():
            return await self.client.ack(physical_queue, ids)

    # ....................... #

    async def nack(
        self,
        queue: str,
        ids: Sequence[str],
        *,
        requeue: bool = True,
        count: bool = True,
    ) -> int:
        # ``count=False`` requeues a byte-identical copy so the broker's receive tally
        # resets — always on a standard queue, on FIFO once the tally nears the redrive
        # threshold (the order-preserving reset is kept while it is safe) — so a drain
        # refusal or key-outage redelivery cannot creep into the redrive DLQ; see
        # ``SQSClient.nack``.
        physical_queue = await self.__queue_name(queue)

        async with self.client.client():
            return await self.client.nack(physical_queue, ids, requeue=requeue, count=count)
