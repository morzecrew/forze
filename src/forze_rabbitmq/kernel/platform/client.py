from forze_rabbitmq._compat import require_rabbitmq

require_rabbitmq()

# ....................... #

import asyncio
from contextlib import asynccontextmanager
from contextvars import ContextVar
from datetime import datetime, timedelta, timezone
from typing import AsyncIterator, Mapping, Optional, Sequence, final
from uuid import uuid4

import attrs
from aio_pika import DeliveryMode, Message, connect_robust
from aio_pika.abc import (
    AbstractChannel,
    AbstractIncomingMessage,
    AbstractQueue,
    AbstractRobustConnection,
)

from forze.base.errors import InfrastructureError

from .errors import rabbitmq_handled
from .types import RabbitMQQueueMessage

# ----------------------- #

_KEY_HEADER = "forze_key"

# ....................... #


@final
@attrs.define(frozen=True, slots=True, kw_only=True)
class RabbitMQConfig:
    heartbeat: int = 60
    connect_timeout: Optional[float] = 5.0
    queue_durable: bool = True
    persistent_messages: bool = True
    publisher_confirms: bool = True
    prefetch_count: int = 100


# ....................... #


@final
@attrs.define(slots=True)
class RabbitMQClient:
    __connection: Optional[AbstractRobustConnection] = attrs.field(
        default=None, init=False
    )
    __config: RabbitMQConfig = attrs.field(factory=RabbitMQConfig, init=False)

    __ctx_channel: ContextVar[Optional[AbstractChannel]] = attrs.field(
        factory=lambda: ContextVar("rabbitmq_channel", default=None),
        init=False,
    )
    __ctx_depth: ContextVar[int] = attrs.field(
        factory=lambda: ContextVar("rabbitmq_channel_depth", default=0),
        init=False,
    )

    __pending: dict[str, tuple[str, AbstractIncomingMessage]] = attrs.field(
        factory=dict,
        init=False,
    )
    __pending_lock: asyncio.Lock = attrs.field(factory=asyncio.Lock, init=False)
    __pending_channel: Optional[AbstractChannel] = attrs.field(
        default=None,
        init=False,
    )
    __pending_channel_lock: asyncio.Lock = attrs.field(
        factory=asyncio.Lock,
        init=False,
    )

    # ....................... #
    # Lifecycle

    async def initialize(
        self,
        dsn: str,
        *,
        config: RabbitMQConfig = RabbitMQConfig(),
    ) -> None:
        if self.__connection is not None and not self.__connection.is_closed:
            return

        self.__config = config
        self.__connection = await connect_robust(
            dsn,
            timeout=config.connect_timeout,
            heartbeat=config.heartbeat,
        )

    # ....................... #

    async def close(self) -> None:
        if self.__pending_channel is not None and not self.__pending_channel.is_closed:
            await self.__pending_channel.close()

        self.__pending_channel = None

        if self.__connection is not None and not self.__connection.is_closed:
            await self.__connection.close()

        self.__connection = None

        async with self.__pending_lock:
            self.__pending.clear()

    # ....................... #

    def __require_connection(self) -> AbstractRobustConnection:
        if self.__connection is None or self.__connection.is_closed:
            raise InfrastructureError("RabbitMQ client is not initialized")

        return self.__connection

    # ....................... #

    async def health(self) -> tuple[str, bool]:
        try:
            channel = await self.__require_connection().channel()
            await channel.close()

            return "ok", True
        except Exception as e:
            return str(e), False

    # ....................... #
    # Context helpers

    def __current_channel(self) -> Optional[AbstractChannel]:
        return self.__ctx_channel.get()

    # ....................... #

    @rabbitmq_handled("rabbitmq.channel")
    @asynccontextmanager
    async def channel(self) -> AsyncIterator[AbstractChannel]:
        depth = self.__ctx_depth.get()
        parent = self.__current_channel()

        if depth > 0 and parent is not None and not parent.is_closed:
            token_depth = self.__ctx_depth.set(depth + 1)
            try:
                yield parent
            finally:
                self.__ctx_depth.reset(token_depth)

            return

        channel = await self.__require_connection().channel(
            publisher_confirms=self.__config.publisher_confirms
        )

        if self.__config.prefetch_count > 0:
            await channel.set_qos(prefetch_count=self.__config.prefetch_count)

        token_channel = self.__ctx_channel.set(channel)
        token_depth = self.__ctx_depth.set(1)

        try:
            yield channel

        finally:
            self.__ctx_depth.reset(token_depth)
            self.__ctx_channel.reset(token_channel)

            if not channel.is_closed:
                await channel.close()

    # ....................... #
    # Message helpers

    async def __declare_queue(
        self,
        channel: AbstractChannel,
        queue: str,
    ) -> AbstractQueue:
        return await channel.declare_queue(
            queue,
            durable=self.__config.queue_durable,
        )

    # ....................... #

    async def __require_pending_channel(self) -> AbstractChannel:
        async with self.__pending_channel_lock:
            if (
                self.__pending_channel is not None
                and not self.__pending_channel.is_closed
            ):
                return self.__pending_channel

            channel = await self.__require_connection().channel(
                publisher_confirms=False
            )

            if self.__config.prefetch_count > 0:
                await channel.set_qos(prefetch_count=self.__config.prefetch_count)

            self.__pending_channel = channel

            return channel

    # ....................... #

    @staticmethod
    def __extract_key(headers: Optional[Mapping[str, object]]) -> Optional[str]:
        if not headers:
            return None

        raw_key = headers.get(_KEY_HEADER)

        if isinstance(raw_key, bytes):
            return raw_key.decode("utf-8")

        if isinstance(raw_key, str):
            return raw_key

        return None

    # ....................... #

    @staticmethod
    def __extract_timestamp(raw_timestamp: object) -> Optional[datetime]:
        if isinstance(raw_timestamp, datetime):
            return raw_timestamp

        if isinstance(raw_timestamp, (int, float)):
            return datetime.fromtimestamp(raw_timestamp, tz=timezone.utc)

        return None

    # ....................... #

    async def __next_message_id(
        self,
        queue: str,
        message: AbstractIncomingMessage,
    ) -> str:
        base = message.message_id or (
            f"{queue}:{message.delivery_tag}"
            if message.delivery_tag is not None
            else uuid4().hex
        )
        candidate = base
        suffix = 1

        async with self.__pending_lock:
            while candidate in self.__pending:
                suffix += 1
                candidate = f"{base}:{suffix}"

            self.__pending[candidate] = (queue, message)

        return candidate

    # ....................... #

    async def __to_message(
        self,
        queue: str,
        raw: AbstractIncomingMessage,
    ) -> RabbitMQQueueMessage:
        message_id = await self.__next_message_id(queue, raw)

        return RabbitMQQueueMessage(
            queue=queue,
            id=message_id,
            body=raw.body,
            type=raw.type,
            enqueued_at=self.__extract_timestamp(raw.timestamp),
            key=self.__extract_key(raw.headers),
        )

    # ....................... #
    # Canonical queue methods

    @rabbitmq_handled("rabbitmq.enqueue")
    async def enqueue(
        self,
        queue: str,
        body: bytes,
        *,
        type: Optional[str] = None,
        key: Optional[str] = None,
        enqueued_at: Optional[datetime] = None,
        message_id: Optional[str] = None,
    ) -> str:
        return (
            await self.enqueue_many(
                queue,
                [body],
                type=type,
                key=key,
                enqueued_at=enqueued_at,
                message_ids=[message_id] if message_id is not None else None,
            )
        )[0]

    # ....................... #

    @rabbitmq_handled("rabbitmq.enqueue_many")
    async def enqueue_many(
        self,
        queue: str,
        bodies: Sequence[bytes],
        *,
        type: Optional[str] = None,
        key: Optional[str] = None,
        enqueued_at: Optional[datetime] = None,
        message_ids: Optional[Sequence[str]] = None,
    ) -> list[str]:
        if not bodies:
            return []

        if message_ids is not None and len(message_ids) != len(bodies):
            raise InfrastructureError(
                "RabbitMQ message_ids size must match batch body size"
            )

        resolved_ids = (
            list(message_ids)
            if message_ids is not None
            else [uuid4().hex for _ in range(len(bodies))]
        )
        headers = None

        if key is not None:
            headers = {_KEY_HEADER: key}
        delivery_mode = (
            DeliveryMode.PERSISTENT
            if self.__config.persistent_messages
            else DeliveryMode.NOT_PERSISTENT
        )

        async with self.channel() as channel:
            await self.__declare_queue(channel, queue)

            for body, resolved_message_id in zip(bodies, resolved_ids, strict=True):
                message = Message(
                    body=body,
                    content_type="application/json",
                    delivery_mode=delivery_mode,
                    message_id=resolved_message_id,
                    timestamp=enqueued_at,
                    type=type,
                    headers=headers,  # pyright: ignore[reportArgumentType]
                )

                await channel.default_exchange.publish(message, routing_key=queue)

        return resolved_ids

    # ....................... #

    @rabbitmq_handled("rabbitmq.receive")
    async def receive(
        self,
        queue: str,
        *,
        limit: Optional[int] = None,
        timeout: Optional[timedelta] = None,
    ) -> list[RabbitMQQueueMessage]:
        max_messages = 1 if limit is None else limit

        if max_messages <= 0:
            return []

        timeout_seconds = timeout.total_seconds() if timeout is not None else 0
        out: list[RabbitMQQueueMessage] = []
        channel = await self.__require_pending_channel()
        declared = await self.__declare_queue(channel, queue)

        while len(out) < max_messages:
            raw = await declared.get(
                no_ack=False,
                fail=False,
                timeout=timeout_seconds,
            )

            if raw is None:
                break

            out.append(await self.__to_message(queue, raw))

        return out

    # ....................... #

    @rabbitmq_handled("rabbitmq.consume")
    async def consume(
        self,
        queue: str,
        *,
        timeout: Optional[timedelta] = None,
    ) -> AsyncIterator[RabbitMQQueueMessage]:
        timeout_seconds = timeout.total_seconds() if timeout is not None else 1.0
        channel = await self.__require_pending_channel()
        declared = await self.__declare_queue(channel, queue)

        while True:
            raw = await declared.get(
                no_ack=False,
                fail=False,
                timeout=timeout_seconds,
            )

            if raw is None:
                continue

            yield await self.__to_message(queue, raw)

    # ....................... #

    async def __pending_by_ids(
        self,
        queue: str,
        ids: Sequence[str],
    ) -> list[tuple[str, AbstractIncomingMessage]]:
        async with self.__pending_lock:
            out: list[tuple[str, AbstractIncomingMessage]] = []

            for message_id in ids:
                entry = self.__pending.get(message_id)

                if entry is None:
                    continue

                pending_queue, message = entry

                if pending_queue != queue:
                    continue

                out.append((message_id, message))

            return out

    # ....................... #

    async def __drop_pending(self, message_id: str) -> None:
        async with self.__pending_lock:
            self.__pending.pop(message_id, None)

    # ....................... #

    @rabbitmq_handled("rabbitmq.ack")
    async def ack(self, queue: str, ids: Sequence[str]) -> int:
        if not ids:
            return 0

        messages = await self.__pending_by_ids(queue, ids)
        acked = 0

        for message_id, message in messages:
            await message.ack()
            await self.__drop_pending(message_id)
            acked += 1

        return acked

    # ....................... #

    @rabbitmq_handled("rabbitmq.nack")
    async def nack(
        self,
        queue: str,
        ids: Sequence[str],
        *,
        requeue: bool = True,
    ) -> int:
        if not ids:
            return 0

        messages = await self.__pending_by_ids(queue, ids)
        nacked = 0

        for message_id, message in messages:
            await message.nack(requeue=requeue)
            await self.__drop_pending(message_id)
            nacked += 1

        return nacked
