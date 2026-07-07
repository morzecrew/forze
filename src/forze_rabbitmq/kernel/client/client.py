from pydantic import SecretStr

from forze_rabbitmq._compat import require_rabbitmq

require_rabbitmq()

# ....................... #

import asyncio
from contextlib import asynccontextmanager, suppress
from datetime import datetime, timedelta, timezone
from typing import Any, AsyncGenerator, Mapping, Sequence, final

import attrs
from aio_pika import DeliveryMode, ExchangeType, Message, connect_robust
from aio_pika.abc import (
    AbstractChannel,
    AbstractIncomingMessage,
    AbstractQueue,
    AbstractRobustConnection,
)

from forze.application.contracts.queue import resolve_delivery_delay
from forze.base.exceptions import exc
from forze.base.primitives import ContextScopedResource, GuardedLifecycle, uuid4

from .._logger import logger
from .errors import exc_interceptor
from .port import RabbitMQClientPort
from .types import RabbitMQQueueMessage
from .value_objects import RabbitMQConfig

# ----------------------- #

_KEY_HEADER = "forze_key"
"""Reserved AMQP header carrying the partitioning key; wins over caller headers."""

_RESERVED_HEADERS = frozenset({_KEY_HEADER})
"""Header names owned by the transport: caller values are overwritten and
the keys are excluded from the caller-visible ``headers`` mapping on read."""

_INTERNAL_HEADER_PREFIX = "x-"
"""Broker-internal AMQP headers (``x-death`` and friends) excluded from the
caller-visible ``headers`` mapping on read."""

_DELAY_QUEUE_SUFFIX = ".__forze_delay"
"""Suffix for the per-delay-value DLX queues paired with a work queue."""

_DLQ_SUFFIX = ".dlq"
"""Suffix for the dead-letter queue bound to a configured ``dead_letter_exchange``."""

_DELIVERY_HEADER = "x-forze-delivery"
"""Client-maintained redelivery counter (survives ``nack(requeue=True)`` republish)."""

_RABBITMQ_MAX_EXPIRATION_MS = 2**32 - 1
"""Upper bound for TTL/expiry values (milliseconds) on the wire."""

_DELAY_QUEUE_EXPIRES_FACTOR = 10
"""Idle ``x-expires`` for a delay queue, as a multiple of its TTL."""

_DELAY_QUEUE_MIN_EXPIRES_MS = 60_000
"""Floor for delay-queue ``x-expires`` so very short delays do not race
queue deletion against in-flight publishes."""

_DELAY_QUEUE_MAX_EXPIRES_GRACE_MS = 24 * 60 * 60 * 1000
"""Cap on how long past its TTL an idle delay queue may linger (24h), so
very long one-off delays do not pin broker resources for ~10x their TTL."""

_DEFAULT_RECEIVE_WINDOW = timedelta(seconds=2)
"""Bounded wait window for :meth:`RabbitMQClient.receive` when *timeout* is unset.

Keeps ``receive(timeout=None)`` a bounded call: it returns whatever messages
arrived within the window instead of blocking until a full batch shows up.
"""

# ....................... #


@final
@attrs.define(slots=True)
class RabbitMQClient(RabbitMQClientPort):
    __connection: AbstractRobustConnection | None = attrs.field(
        default=None, init=False
    )
    __config: RabbitMQConfig = attrs.field(factory=RabbitMQConfig, init=False)

    __channel_scope: ContextScopedResource[AbstractChannel] = attrs.field(
        factory=lambda: ContextScopedResource[AbstractChannel]("rabbitmq_channel"),
        init=False,
    )

    __pending: dict[str, tuple[str, AbstractIncomingMessage]] = attrs.field(
        factory=dict,
        init=False,
    )
    __pending_lock: asyncio.Lock = attrs.field(factory=asyncio.Lock, init=False)
    __pending_channel: AbstractChannel | None = attrs.field(
        default=None,
        init=False,
    )
    __pending_channel_lock: asyncio.Lock = attrs.field(
        factory=asyncio.Lock,
        init=False,
    )
    __pending_watermark_warned: bool = attrs.field(default=False, init=False)
    __lifecycle: GuardedLifecycle = attrs.field(factory=GuardedLifecycle, init=False)

    # ....................... #
    # Lifecycle

    async def initialize(
        self,
        dsn: str | SecretStr,
        *,
        config: RabbitMQConfig = RabbitMQConfig(),
    ) -> None:
        async def setup() -> None:
            resolved_dsn = dsn.get_secret_value() if isinstance(dsn, SecretStr) else dsn

            self.__config = config
            self.__connection = await connect_robust(
                resolved_dsn,
                timeout=config.connect_timeout.total_seconds(),
                heartbeat=config.heartbeat.total_seconds(),
            )
            logger.trace("RabbitMQ connection opened")

        await self.__lifecycle.initialize(
            setup,
            ready=lambda: (
                self.__connection is not None and not self.__connection.is_closed
            ),
        )

    # ....................... #

    async def close(self) -> None:
        await self.__lifecycle.close(self.__teardown)

    # ....................... #

    async def __teardown(self) -> None:
        # Return unacked deliveries to the broker *before* tearing the
        # channel down: nack(requeue=True) makes them redeliverable
        # immediately instead of only after the broker notices the
        # connection drop. Best-effort — close() must never raise.
        await self.__nack_pending_on_close()

        try:
            if (
                self.__pending_channel is not None
                and not self.__pending_channel.is_closed
            ):
                await self.__pending_channel.close()
        except Exception as e:
            logger.warning("RabbitMQ close: pending channel close failed: %s", e)

        self.__pending_channel = None

        try:
            if self.__connection is not None and not self.__connection.is_closed:
                await self.__connection.close()
        except Exception as e:
            logger.warning("RabbitMQ close: connection close failed: %s", e)

        self.__connection = None
        logger.trace("RabbitMQ connection closed")

        async with self.__pending_lock:
            self.__pending.clear()
            self.__pending_watermark_warned = False

    # ....................... #

    async def __nack_pending_on_close(self) -> None:
        """Best-effort ``nack(requeue=True)`` of every pending delivery.

        Failures are logged and swallowed: the broker redelivers unacked
        messages once the connection drops anyway, so a failed nack only
        delays redelivery — it must not turn ``close()`` into an error path.
        """

        async with self.__pending_lock:
            entries = list(self.__pending.items())
            self.__pending.clear()
            self.__pending_watermark_warned = False

        if not entries:
            return

        results = await asyncio.gather(
            *(message.nack(requeue=True) for _, (_, message) in entries),
            return_exceptions=True,
        )

        for (message_id, _), result in zip(entries, results, strict=True):
            if isinstance(result, BaseException):
                logger.warning(
                    "RabbitMQ close: failed to nack pending message %s "
                    "(broker redelivers it after the connection drops): %s",
                    message_id,
                    result,
                )

    # ....................... #

    def __require_connection(self) -> AbstractRobustConnection:
        if self.__connection is None or self.__connection.is_closed:
            raise exc.internal("RabbitMQ client is not initialized")

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

    async def __open_channel(self) -> AbstractChannel:
        channel = await self.__require_connection().channel(
            publisher_confirms=self.__config.publisher_confirms
        )

        if self.__config.prefetch_count > 0:
            await channel.set_qos(prefetch_count=self.__config.prefetch_count)

        return channel

    # ....................... #

    @staticmethod
    async def __close_channel(channel: AbstractChannel) -> None:
        if not channel.is_closed:
            await channel.close()

    # ....................... #

    @exc_interceptor.asynccontextmanager("rabbitmq.channel")  # type: ignore[untyped-decorator]
    @asynccontextmanager
    async def channel(self) -> AsyncGenerator[AbstractChannel]:
        async with self.__channel_scope.scope(
            self.__open_channel,
            closer=self.__close_channel,
            reusable=lambda channel: not channel.is_closed,
        ) as channel:
            yield channel

    # ....................... #
    # Message helpers

    async def __declare_queue(
        self,
        channel: AbstractChannel,
        queue: str,
    ) -> AbstractQueue:
        if self.__config.dead_letter_exchange is None:
            # Unchanged declaration (no arguments) when no poison sink is configured.
            return await channel.declare_queue(
                queue,
                durable=self.__config.queue_durable,
            )

        await self.__ensure_dead_letter(channel)
        arguments: dict[str, Any] = {
            "x-dead-letter-exchange": self.__config.dead_letter_exchange
        }

        return await channel.declare_queue(
            queue,
            durable=self.__config.queue_durable,
            arguments=arguments,
        )

    # ....................... #

    async def __ensure_dead_letter(self, channel: AbstractChannel) -> None:
        """Declare the configured DLX (fanout) + a bound durable dead-letter queue.

        A message rejected on a work queue (``nack(requeue=False)`` — an undecodable /
        schema-drift message) is dead-lettered to the DLX and lands in ``<dlx>.dlq`` rather than
        being silently discarded. Idempotent (declares are declarative).
        """

        dlx = self.__config.dead_letter_exchange

        if dlx is None:
            return

        exchange = await channel.declare_exchange(
            dlx, ExchangeType.FANOUT, durable=self.__config.queue_durable
        )
        dlq = await channel.declare_queue(
            f"{dlx}{_DLQ_SUFFIX}", durable=self.__config.queue_durable
        )
        await dlq.bind(exchange)

    # ....................... #

    @staticmethod
    def _delay_queue_name(work_queue: str, delay_ms: int) -> str:
        return f"{work_queue}{_DELAY_QUEUE_SUFFIX}.{delay_ms}"

    # ....................... #

    @staticmethod
    def _delay_queue_expires_ms(delay_ms: int) -> int:
        """Idle ``x-expires`` for the delay queue holding ``delay_ms`` messages.

        Roughly 10x the TTL, floored at one minute and capped at TTL + 24h,
        and always within the wire-level 32-bit millisecond bound. The result
        is always >= the TTL (declaration at publish time resets the idle
        timer), so a delay queue never disappears under messages still
        waiting to expire.
        """

        expires = max(
            _DELAY_QUEUE_MIN_EXPIRES_MS,
            delay_ms * _DELAY_QUEUE_EXPIRES_FACTOR,
        )
        expires = min(expires, delay_ms + _DELAY_QUEUE_MAX_EXPIRES_GRACE_MS)

        return min(expires, _RABBITMQ_MAX_EXPIRATION_MS)

    # ....................... #

    async def __ensure_delay_queue(
        self,
        channel: AbstractChannel,
        work_queue: str,
        delay_ms: int,
    ) -> str:
        """Declare the per-delay-value DLX queue and return its name.

        Delayed delivery uses one delay queue *per distinct delay value*
        (``<queue>.__forze_delay.<ms>``) declared with a queue-level
        ``x-message-ttl`` instead of per-message expirations. RabbitMQ only
        expires messages from the queue head, so mixing TTLs in a single
        delay queue lets a long delay block shorter ones behind it
        (head-of-line blocking); a uniform TTL per queue makes that
        impossible. Expired messages dead-letter through the default
        exchange back into the work queue, and ``x-expires`` lets the broker
        drop delay queues for one-off delay values once idle.

        Deliberately *not* cached: ``x-expires`` may delete an idle delay
        queue between publishes, so re-declaring on every delayed publish is
        required for correctness (it also refreshes the idle timer) and is a
        cheap idempotent operation.
        """

        delay_queue = self._delay_queue_name(work_queue, delay_ms)

        await self.__declare_queue(channel, work_queue)
        await channel.declare_queue(
            delay_queue,
            durable=self.__config.queue_durable,
            arguments={
                "x-dead-letter-exchange": "",
                "x-dead-letter-routing-key": work_queue,
                "x-message-ttl": delay_ms,
                "x-expires": self._delay_queue_expires_ms(delay_ms),
            },
        )

        return delay_queue

    # ....................... #

    @staticmethod
    def _resolve_enqueue_delay(
        *,
        delay: timedelta | None,
        not_before: datetime | None,
        delayed_delivery: bool,
    ) -> timedelta | None:
        resolved = resolve_delivery_delay(delay=delay, not_before=not_before)

        if resolved is None:
            return None

        if not delayed_delivery:
            raise exc.precondition(
                "RabbitMQ delayed enqueue requires delayed_delivery=True on the "
                "queue writer configuration"
            )

        milliseconds = int(resolved.total_seconds() * 1000)

        if milliseconds <= 0:
            return None

        if milliseconds > _RABBITMQ_MAX_EXPIRATION_MS:
            raise exc.precondition(
                "RabbitMQ enqueue delay exceeds the maximum supported delay"
            )

        return timedelta(milliseconds=milliseconds)

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
    def __extract_key(headers: Mapping[str, object] | None) -> str | None:
        if not headers:
            return None

        raw_key = headers.get(_KEY_HEADER)

        if isinstance(raw_key, bytes):
            return raw_key.decode("utf-8")

        return raw_key if isinstance(raw_key, str) else None

    # ....................... #

    @staticmethod
    def __extract_headers(
        headers: Mapping[str, object] | None,
    ) -> dict[str, str] | None:
        """Return caller-visible string headers from raw AMQP headers.

        Reserved transport keys and broker-internal ``x-*`` headers are
        excluded; only string (or UTF-8 bytes) values survive — AMQP allows
        richer types, but the port contract is string-to-string.
        """

        if not headers:
            return None

        out: dict[str, str] = {}

        for raw_key, raw_value in headers.items():
            if raw_key in _RESERVED_HEADERS or raw_key.startswith(
                _INTERNAL_HEADER_PREFIX
            ):
                continue

            if isinstance(raw_value, bytes):
                try:
                    out[raw_key] = raw_value.decode("utf-8")
                except UnicodeDecodeError:
                    continue

            elif isinstance(raw_value, str):
                out[raw_key] = raw_value

        return out or None

    # ....................... #

    @staticmethod
    def __extract_delivery_count(raw: AbstractIncomingMessage) -> int:
        """Approximate deliveries of *raw* including this one.

        Best-effort: ``x-death`` entries with reason ``rejected`` count
        dead-letter redelivery cycles (DLX retry topologies), so their summed
        ``count`` + 1 is the delivery number. Without ``x-death`` history the
        broker only exposes the boolean ``redelivered`` flag, so a redelivered
        message reports ``2`` even when it was delivered more often. ``expired``
        x-death entries (the delayed-delivery DLX hop) are not deliveries and
        are ignored.
        """

        headers = raw.headers or {}

        # When redelivery counting is enabled, a client-maintained ``x-forze-delivery`` counter
        # survives ``nack(requeue=True)`` (a plain broker requeue never advances the count past the
        # ``redelivered``-flag ceiling of 2). The header records prior deliveries; +1 for this one.
        forze_delivery = headers.get(_DELIVERY_HEADER)
        if isinstance(forze_delivery, int):
            return forze_delivery + 1

        x_death = headers.get("x-death")
        rejected = 0

        if isinstance(x_death, (list, tuple)):
            for entry in x_death:  # pyright: ignore[reportUnknownVariableType]
                if not isinstance(entry, Mapping):
                    continue

                reason = entry.get(
                    "reason"
                )  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]

                if isinstance(reason, bytes):
                    reason = reason.decode("utf-8", errors="replace")

                if reason != "rejected":
                    continue

                count = entry.get(
                    "count"
                )  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]

                if isinstance(count, int):
                    rejected += count

        if rejected > 0:
            return rejected + 1

        return 2 if raw.redelivered else 1

    # ....................... #

    @staticmethod
    def __extract_timestamp(raw_timestamp: object) -> datetime | None:
        if isinstance(raw_timestamp, datetime):
            return raw_timestamp

        if isinstance(raw_timestamp, (int, float)):
            return datetime.fromtimestamp(raw_timestamp, tz=timezone.utc)

        return None

    # ....................... #

    def __check_pending_watermark_locked(self) -> None:
        """Warn once when the pending map crosses the configured watermark.

        The threshold is :attr:`RabbitMQConfig.pending_watermark` — a soft
        cap for observability, never enforcement. Must be called with
        ``__pending_lock`` held. Re-arms once the map drains back to half
        the watermark so a long-lived process that recovers and leaks again
        warns again.
        """

        size = len(self.__pending)
        watermark = self.__config.pending_watermark

        if size > watermark:
            if not self.__pending_watermark_warned:
                self.__pending_watermark_warned = True
                logger.warning(
                    "RabbitMQ pending-delivery map exceeds %d entries (%d): "
                    "deliveries are likely leaking without ack/nack (e.g. "
                    "handler crashes between receive and ack); they stay "
                    "invisible to other consumers until this client closes",
                    watermark,
                    size,
                )
        elif size <= watermark // 2:
            self.__pending_watermark_warned = False

    # ....................... #

    async def __register_pending_batch(
        self,
        queue: str,
        raws: list[AbstractIncomingMessage],
    ) -> list[str]:
        """Register multiple messages atomically under a single lock acquisition."""

        async with self.__pending_lock:
            ids: list[str] = []

            for raw in raws:
                base = raw.message_id or (
                    f"{queue}:{raw.delivery_tag}"
                    if raw.delivery_tag is not None
                    else uuid4().hex
                )
                candidate = base
                suffix = 1

                while candidate in self.__pending:
                    suffix += 1
                    candidate = f"{base}:{suffix}"

                self.__pending[candidate] = (queue, raw)
                ids.append(candidate)

            self.__check_pending_watermark_locked()

            return ids

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
            self.__check_pending_watermark_locked()

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
            headers=self.__extract_headers(raw.headers),
            delivery_count=self.__extract_delivery_count(raw),
        )

    # ....................... #

    async def __to_message_batch(
        self,
        queue: str,
        raws: list[AbstractIncomingMessage],
    ) -> list[RabbitMQQueueMessage]:
        message_ids = await self.__register_pending_batch(queue, raws)

        rmq_messages: list[RabbitMQQueueMessage] = []

        for message_id, raw in zip(message_ids, raws, strict=True):
            m = RabbitMQQueueMessage(
                queue=queue,
                id=message_id,
                body=raw.body,
                type=raw.type,
                enqueued_at=self.__extract_timestamp(raw.timestamp),
                key=self.__extract_key(raw.headers),
                headers=self.__extract_headers(raw.headers),
                delivery_count=self.__extract_delivery_count(raw),
            )
            rmq_messages.append(m)

        return rmq_messages

    # ....................... #
    # Canonical queue methods

    # ``enqueue`` deliberately delegates to ``enqueue_many`` so queue
    # declaration, delay-queue routing, and expiration handling live in a
    # single publish path; a standalone rewrite would only duplicate it.
    @exc_interceptor.coroutine("rabbitmq.enqueue")  # type: ignore[untyped-decorator]
    async def enqueue(
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
        delayed_delivery: bool = False,
        headers: Mapping[str, str] | None = None,
    ) -> str:
        """Publish one message; see :meth:`enqueue_many` for delay semantics."""
        return (
            await self.enqueue_many(
                queue,
                [body],
                type=type,
                key=key,
                enqueued_at=enqueued_at,
                message_ids=[message_id] if message_id is not None else None,
                delay=delay,
                not_before=not_before,
                delayed_delivery=delayed_delivery,
                headers=headers,
            )
        )[0]

    # ....................... #

    @exc_interceptor.coroutine("rabbitmq.enqueue_many")  # type: ignore[untyped-decorator]
    async def enqueue_many(
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
        delayed_delivery: bool = False,
        headers: Mapping[str, str] | None = None,
        message_headers: Sequence[Mapping[str, str]] | None = None,
    ) -> list[str]:
        """Publish a batch of messages, optionally with delayed delivery.

        Caller *headers* ride the AMQP message headers verbatim; the reserved
        transport keys (``forze_key``) always win on collision.
        *message_headers*, when given, supplies one header mapping per body (its
        length must equal *bodies*); publish ``i`` carries
        ``{**(headers or {}), **message_headers[i]}`` so a single batched
        publish can give each message distinct headers (e.g. a per-message
        ``forze_event_id``). ``None`` keeps every publish on the shared
        *headers* (byte-for-byte unchanged).

        Delayed delivery uses the standard RabbitMQ per-TTL-queue pattern:
        each distinct delay value gets its own DLX queue
        (``<queue>.__forze_delay.<delay_ms>``) declared with a queue-level
        ``x-message-ttl`` equal to the delay, dead-lettering back into the
        work queue, and ``x-expires`` so one-off delay values do not
        accumulate queues forever. Because RabbitMQ only expires messages
        from the queue head, a *single* shared delay queue with per-message
        TTLs would let a long delay block shorter ones enqueued after it
        (head-of-line blocking); one uniform-TTL queue per delay value makes
        that impossible while keeping same-delay messages FIFO.
        """
        if not bodies:
            return []

        if message_ids is not None and len(message_ids) != len(bodies):
            raise exc.precondition(
                "RabbitMQ message_ids size must match batch body size"
            )

        if message_headers is not None and len(message_headers) != len(bodies):
            raise exc.precondition(
                "RabbitMQ message_headers size must match batch body size"
            )

        resolved_ids = (
            list(message_ids)
            if message_ids is not None
            else [uuid4().hex for _ in range(len(bodies))]
        )

        def __build_amqp_headers(
            base: Mapping[str, str] | None,
        ) -> dict[str, str] | None:
            # Strip reserved transport keys from caller input — only the transport sets them.
            # This must hold even when ``key`` is ``None`` (nothing is written): otherwise a
            # caller-supplied ``forze_key`` would silently set the partitioning lane.
            filtered = (
                {k: v for k, v in base.items() if k not in _RESERVED_HEADERS}
                if base
                else {}
            )
            built: dict[str, str] | None = filtered or None

            if key is not None:
                built = built or {}
                built[_KEY_HEADER] = key

            return built

        # Per-message effective headers (shared headers overridden by the
        # per-message entry) when message_headers is given; otherwise every
        # publish rides the shared batch-wide headers (unchanged path).
        if message_headers is not None:
            per_message_amqp_headers = [
                __build_amqp_headers({**(headers or {}), **mh})
                for mh in message_headers
            ]
        else:
            per_message_amqp_headers = [__build_amqp_headers(headers)] * len(bodies)

        delivery_mode = (
            DeliveryMode.PERSISTENT
            if self.__config.persistent_messages
            else DeliveryMode.NOT_PERSISTENT
        )

        # Delay is realized via the per-delay-value DLX queue's queue-level
        # ``x-message-ttl`` (see ``__ensure_delay_queue``), not a per-message
        # ``expiration``: per-message TTLs only expire from the queue head,
        # so a long delay published first would hold shorter ones back.
        resolved_delay = self._resolve_enqueue_delay(
            delay=delay,
            not_before=not_before,
            delayed_delivery=delayed_delivery,
        )
        messages: list[Message] = []

        for body, resolved_message_id, amqp_headers in zip(
            bodies, resolved_ids, per_message_amqp_headers, strict=True
        ):
            message = Message(
                body=body,
                content_type="application/json",
                delivery_mode=delivery_mode,
                message_id=resolved_message_id,
                timestamp=enqueued_at,
                type=type,
                headers=amqp_headers,  # type: ignore[arg-type]
            )
            messages.append(message)

        async with self.channel() as channel:
            publish_queue = queue

            if resolved_delay is not None:
                delay_ms = int(resolved_delay.total_seconds() * 1000)
                publish_queue = await self.__ensure_delay_queue(
                    channel, queue, delay_ms
                )
            else:
                await self.__declare_queue(channel, queue)

            await asyncio.gather(
                *(
                    channel.default_exchange.publish(
                        message,
                        routing_key=publish_queue,
                    )
                    for message in messages
                )
            )

        return resolved_ids

    # ....................... #

    @exc_interceptor.coroutine("rabbitmq.receive")  # type: ignore[untyped-decorator]
    async def receive(
        self,
        queue: str,
        *,
        limit: int | None = None,
        timeout: timedelta | None = None,
    ) -> list[RabbitMQQueueMessage]:
        """Fetch up to ``limit`` messages within a bounded wait window.

        ``timeout`` caps the **total** wait; ``None`` (or a non-positive
        value) falls back to :data:`_DEFAULT_RECEIVE_WINDOW`. The call
        returns early once ``limit`` messages arrived and otherwise
        returns whatever arrived when the window elapses (possibly none).
        """
        max_messages = 1 if limit is None else limit

        if max_messages <= 0:
            return []

        window = (
            timeout
            if timeout is not None and timeout.total_seconds() > 0
            else _DEFAULT_RECEIVE_WINDOW
        )
        raw_messages: list[AbstractIncomingMessage] = []

        channel = await self.__require_pending_channel()
        declared = await self.__declare_queue(channel, queue)

        with suppress(TimeoutError):
            async with asyncio.timeout(window.total_seconds()):
                # No iterator timeout: the surrounding ``asyncio.timeout``
                # bounds the whole drain loop (aio_pika treats its iterator
                # ``timeout`` as a per-``__anext__`` wait, not a total one).
                async with declared.iterator(no_ack=False) as it:
                    async for raw in it:
                        raw_messages.append(raw)

                        if len(raw_messages) >= max_messages:
                            break

        return await self.__to_message_batch(queue, raw_messages)

    # ....................... #

    @exc_interceptor.asyncgenerator("rabbitmq.consume")  # type: ignore[untyped-decorator]
    async def consume(
        self,
        queue: str,
        *,
        timeout: timedelta | None = None,
    ) -> AsyncGenerator[RabbitMQQueueMessage]:
        """Yield messages continuously from ``queue``.

        ``timeout`` is an **idle** timeout: ``None`` (or a non-positive
        value) consumes forever, yielding messages as they arrive; a finite
        value stops the generator cleanly (no error) once no message has
        arrived for that duration. Each message resets the idle window.
        """
        idle_seconds = (
            timeout.total_seconds()
            if timeout is not None and timeout.total_seconds() > 0
            else None
        )
        channel = await self.__require_pending_channel()
        declared = await self.__declare_queue(channel, queue)

        # aio_pika applies ``timeout`` per ``__anext__`` wait, which matches
        # idle-timeout semantics; ``None`` waits unbounded (consume forever).
        async with declared.iterator(timeout=idle_seconds, no_ack=False) as it:
            while True:
                try:
                    raw = await it.__anext__()

                except (StopAsyncIteration, TimeoutError):
                    return

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

    async def __drop_pending_many(self, message_ids: Sequence[str]) -> None:
        async with self.__pending_lock:
            for mid in message_ids:
                self.__pending.pop(mid, None)

            self.__check_pending_watermark_locked()

    # ....................... #

    @exc_interceptor.coroutine("rabbitmq.ack")  # type: ignore[untyped-decorator]
    async def ack(self, queue: str, ids: Sequence[str]) -> int:
        if not ids:
            return 0

        messages = await self.__pending_by_ids(queue, ids)

        if not messages:
            return 0

        await asyncio.gather(*(message.ack() for _, message in messages))
        acked_ids = [message_id for message_id, _ in messages]

        await self.__drop_pending_many(acked_ids)

        return len(acked_ids)

    # ....................... #

    @exc_interceptor.coroutine("rabbitmq.nack")  # type: ignore[untyped-decorator]
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

        if not messages:
            return 0

        if requeue and self.__config.redelivery_counting:
            await self.__requeue_counted(queue, [raw for _, raw in messages])
        else:
            await asyncio.gather(
                *(message.nack(requeue=requeue) for _, message in messages)
            )

        nacked_ids = [message_id for message_id, _ in messages]

        await self.__drop_pending_many(nacked_ids)

        return len(nacked_ids)

    # ....................... #

    async def __requeue_counted(
        self, queue: str, raws: Sequence[AbstractIncomingMessage]
    ) -> None:
        """Requeue by republishing each message with an incremented ``x-forze-delivery`` header,
        then ack the original — so the delivery count survives the requeue (making
        ``max_deliveries`` parking reachable). Publish-then-ack preserves at-least-once: a crash in
        the window redelivers the original *and* the republished copy, which consumer inbox dedup
        collapses (the message id is preserved). Moves the message to the queue tail.
        """

        delivery_mode = (
            DeliveryMode.PERSISTENT
            if self.__config.persistent_messages
            else DeliveryMode.NOT_PERSISTENT
        )

        async with self.channel() as channel:
            await self.__declare_queue(channel, queue)
            await asyncio.gather(
                *(
                    channel.default_exchange.publish(
                        self.__with_incremented_delivery(raw, delivery_mode),
                        routing_key=queue,
                    )
                    for raw in raws
                )
            )

        # Ack the originals only after the republished copies are on the broker.
        await asyncio.gather(*(raw.ack() for raw in raws))

    @staticmethod
    def __with_incremented_delivery(
        raw: AbstractIncomingMessage, delivery_mode: DeliveryMode
    ) -> Message:
        headers = dict(raw.headers or {})
        prior = headers.get(_DELIVERY_HEADER)
        headers[_DELIVERY_HEADER] = (prior + 1) if isinstance(prior, int) else 1

        return Message(
            body=raw.body,
            content_type=raw.content_type or "application/json",
            delivery_mode=delivery_mode,
            message_id=raw.message_id,  # preserved so consumer inbox dedup still collapses copies
            timestamp=raw.timestamp,
            type=raw.type,
            headers=headers,  # type: ignore[arg-type]
        )
