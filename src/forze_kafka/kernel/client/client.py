from forze_kafka._compat import require_kafka

require_kafka()

# ....................... #

import asyncio
from collections.abc import Mapping, Sequence
from contextlib import suppress
from typing import Any, final

import attrs
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, ConsumerRebalanceListener
from aiokafka.admin import AIOKafkaAdminClient
from aiokafka.structs import RecordMetadata

from forze.base.exceptions import exc
from forze.base.primitives import GuardedLifecycle

from .._logger import logger
from ..rebalance import KafkaCommitRebalanceListener
from .errors import exc_interceptor
from .port import KafkaClientPort
from .value_objects import KafkaConfig

# ----------------------- #

_ConsumerKey = tuple[str, str, tuple[str, ...]]
"""Pool key for a data-plane consumer: ``(group, member, sorted topics)``."""


# ....................... #


@final
@attrs.define(slots=True)
class KafkaClient(KafkaClientPort):
    """Owns a shared producer + admin client and a pool of consumers.

    The producer and admin client are started once in :meth:`initialize` and
    stopped in :meth:`close`. Data-plane consumers are created lazily per
    ``(group, member, topics)`` and pooled so the same instance serves a read
    and its follow-up commit; :meth:`close` stops every pooled consumer. Replay
    / lag inspection uses short-lived :meth:`new_transient_consumer` handles the
    caller stops itself.
    """

    __bootstrap: str = attrs.field(default="", init=False)
    __config: KafkaConfig = attrs.field(factory=KafkaConfig, init=False)
    __producer: AIOKafkaProducer | None = attrs.field(default=None, init=False)
    __admin: AIOKafkaAdminClient | None = attrs.field(default=None, init=False)
    __consumers: dict[_ConsumerKey, AIOKafkaConsumer] = attrs.field(factory=dict, init=False)
    __consumer_lock: asyncio.Lock = attrs.field(factory=asyncio.Lock, init=False)
    __lifecycle: GuardedLifecycle = attrs.field(factory=GuardedLifecycle, init=False)

    # ....................... #
    # Lifecycle

    async def initialize(
        self,
        bootstrap_servers: str,
        *,
        config: KafkaConfig = KafkaConfig(),
    ) -> None:
        async def setup() -> None:
            self.__bootstrap = bootstrap_servers
            self.__config = config

            producer = AIOKafkaProducer(
                bootstrap_servers=bootstrap_servers,
                acks=config.acks,
                enable_idempotence=config.enable_idempotence,
                compression_type=config.compression_type,
                linger_ms=config.linger_ms,
                request_timeout_ms=int(config.request_timeout.total_seconds() * 1000),
                **self.__security_kwargs(),
            )
            admin = AIOKafkaAdminClient(
                bootstrap_servers=bootstrap_servers,
                request_timeout_ms=int(config.request_timeout.total_seconds() * 1000),
                **self.__security_kwargs(),
            )

            await producer.start()
            try:
                await admin.start()
            except BaseException:  # pragma: no cover - admin start is an infra fault
                # Roll back the started producer so a failed init leaves no live
                # client behind and the ready check stays False.
                with suppress(Exception):
                    await producer.stop()
                raise

            # Publish both only once both started — the ready lambda then flips.
            self.__producer = producer
            self.__admin = admin

            logger.trace("Kafka client started")

        await self.__lifecycle.initialize(
            setup,
            ready=lambda: self.__producer is not None,
        )

    # ....................... #

    async def close(self) -> None:
        await self.__lifecycle.close(self.__teardown)

    # ....................... #

    async def __teardown(self) -> None:
        async with self.__consumer_lock:
            consumers = list(self.__consumers.values())
            self.__consumers.clear()

        for consumer in consumers:
            try:
                await consumer.stop()
            except Exception:  # pragma: no cover - close must never raise
                logger.warning("Kafka close: consumer stop failed", exc_info=True)

        if self.__producer is not None:
            try:
                await self.__producer.stop()
            except Exception:  # pragma: no cover - close must never raise
                logger.warning("Kafka close: producer stop failed", exc_info=True)
            self.__producer = None

        if self.__admin is not None:
            try:
                await self.__admin.close()
            except Exception:  # pragma: no cover - close must never raise
                logger.warning("Kafka close: admin close failed", exc_info=True)
            self.__admin = None

        logger.trace("Kafka client closed")

    # ....................... #

    def __security_kwargs(self) -> dict[str, Any]:
        kwargs: dict[str, Any] = {"security_protocol": self.__config.security_protocol}

        if self.__config.sasl_mechanism is not None:  # pragma: no cover - needs a SASL broker
            kwargs["sasl_mechanism"] = self.__config.sasl_mechanism
            kwargs["sasl_plain_username"] = self.__config.sasl_plain_username
            kwargs["sasl_plain_password"] = (
                self.__config.sasl_plain_password.get_secret_value()
                if self.__config.sasl_plain_password is not None
                else None
            )

        return kwargs

    # ....................... #

    def __require_producer(self) -> AIOKafkaProducer:
        if self.__producer is None:
            raise exc.configuration("Kafka client is not initialized")

        return self.__producer

    # ....................... #
    # Data plane

    @exc_interceptor.coroutine("kafka.send")
    async def send(
        self,
        topic: str,
        value: bytes,
        *,
        key: bytes | None = None,
        headers: Sequence[tuple[str, bytes]] | None = None,
        timestamp_ms: int | None = None,
    ) -> RecordMetadata:
        return await self.__require_producer().send_and_wait(
            topic,
            value,
            key=key,
            headers=list(headers) if headers else None,
            timestamp_ms=timestamp_ms,
        )

    # ....................... #

    @exc_interceptor.coroutine("kafka.get_consumer")
    async def get_consumer(
        self,
        *,
        group: str,
        member: str,
        topics: Sequence[str],
        auto_offset_reset: str | None = None,
        max_poll_records: int | None = None,
        listener: ConsumerRebalanceListener | None = None,
    ) -> AIOKafkaConsumer:
        key: _ConsumerKey = (group, member, tuple(sorted(topics)))

        async with self.__consumer_lock:
            cached = self.__consumers.get(key)

        if cached is not None:
            return cached

        consumer = AIOKafkaConsumer(
            bootstrap_servers=self.__bootstrap,
            group_id=group,
            client_id=member,
            enable_auto_commit=False,
            auto_offset_reset=auto_offset_reset or self.__config.auto_offset_reset,
            max_poll_records=(
                max_poll_records if max_poll_records is not None else self.__config.max_poll_records
            ),
            request_timeout_ms=int(self.__config.request_timeout.total_seconds() * 1000),
            **self.__security_kwargs(),
        )
        # Subscribe explicitly (not via the constructor's ``*topics``) so a
        # rebalance *listener* rides the subscription: on assign the offset-log
        # consumer rewinds to committed, on revoke it drops stale routing — a
        # routine rebalance then never crashes the commit or skips records.
        consumer.subscribe(topics=list(topics), listener=listener)

        # Bind the live consumer so the listener's assignment seek can act on it;
        # done before start so the first group join already sees it.
        if isinstance(listener, KafkaCommitRebalanceListener):
            listener.consumer = consumer

        # Start outside the lock so an unrelated consumer's slow start does not
        # serialize others; a concurrent racer for the same key is de-duplicated.
        await consumer.start()

        async with self.__consumer_lock:
            winner = self.__consumers.setdefault(key, consumer)

        if winner is not consumer:  # pragma: no cover - concurrent same-key race
            with suppress(Exception):
                await consumer.stop()

        return winner

    # ....................... #

    @exc_interceptor.coroutine("kafka.new_transient_consumer")
    async def new_transient_consumer(
        self,
        *,
        group: str | None = None,
    ) -> AIOKafkaConsumer:
        consumer = AIOKafkaConsumer(
            bootstrap_servers=self.__bootstrap,
            group_id=group,
            enable_auto_commit=False,
            request_timeout_ms=int(self.__config.request_timeout.total_seconds() * 1000),
            **self.__security_kwargs(),
        )
        await consumer.start()

        return consumer

    # ....................... #

    async def admin(self) -> AIOKafkaAdminClient:
        if self.__admin is None:
            raise exc.configuration("Kafka client is not initialized")

        return self.__admin

    # ....................... #

    def group_config(self) -> Mapping[str, object]:
        return {
            "auto_offset_reset": self.__config.auto_offset_reset,
            "max_poll_records": self.__config.max_poll_records,
        }

    # ....................... #

    async def health(self) -> tuple[str, bool]:
        return ("Kafka", self.__producer is not None)
