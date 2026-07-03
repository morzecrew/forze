"""Kafka consumer adapter implementing :class:`CommitStreamGroupQueryPort`."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import AsyncGenerator, Sequence, final
from uuid import UUID

import attrs
from aiokafka import AIOKafkaConsumer
from aiokafka.structs import ConsumerRecord, OffsetAndMetadata, TopicPartition

from forze.application.contracts.stream import (
    CommitStreamGroupCapabilities,
    CommitStreamGroupQueryPort,
    StreamMessage,
    StreamPosition,
)
from forze.application.contracts.tenancy import TenantProviderPort

from ..kernel.client import KafkaClientPort
from ..kernel.relation import NamedResourceSpec, resolve_kafka_topic
from .codecs import KafkaStreamCodec

# ----------------------- #

_DEFAULT_POLL_MS = 1_000
"""``getmany`` wait when the caller passes no timeout (the runner polls anyway)."""


# ....................... #


@attrs.define(slots=True, kw_only=True)
class _ConsumerCell:
    """Mutable holder mapping each group to the consumer its ``read`` used.

    The adapter is frozen; this cell lets a ``read`` record the exact consumer
    instance **per group** so the matching ``commit`` targets the consumer of
    *that* group (``commit`` receives the group but not the member/topics the
    client pools by). Keying by group is what keeps interleaved reads across
    groups from committing one group's offsets through another's consumer."""

    by_group: dict[str, AIOKafkaConsumer] = attrs.field(factory=dict)


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class KafkaCommitStreamGroupAdapter[M](CommitStreamGroupQueryPort[M]):
    """Offset-log consumer over ``AIOKafkaConsumer`` (auto-commit disabled).

    ``read`` fetches a batch via ``getmany`` and surfaces each record's native
    ``(partition, offset)`` on the :class:`StreamMessage`; ``commit`` translates
    processed positions to Kafka's next-offset convention (``offset + 1`` per
    partition) and commits on the same pooled consumer. Auto-commit is never
    used — the commit-stream runner commits only after the inbox mark, so
    at-least-once transport + inbox dedup gives exactly-once effect.
    """

    client: KafkaClientPort
    codec: KafkaStreamCodec[M]
    namespace: NamedResourceSpec
    tenant_aware: bool
    tenant_provider: TenantProviderPort
    auto_offset_reset: str | None = None
    max_poll_records: int | None = None
    _cell: _ConsumerCell = attrs.field(factory=_ConsumerCell, init=False)

    # ....................... #

    def _tenant_id(self) -> UUID | None:
        if not self.tenant_aware:
            return None

        tenant = self.tenant_provider()

        return None if tenant is None else tenant.tenant_id

    # ....................... #

    async def _physical_topics(self, topics: Sequence[str]) -> list[str]:
        tenant_id = self._tenant_id()

        return [
            await resolve_kafka_topic(self.namespace, tenant_id, topic)
            for topic in topics
        ]

    # ....................... #

    def _to_message(self, record: ConsumerRecord[bytes, bytes]) -> StreamMessage[M]:
        headers, message_type = self.codec.decode_headers(record.headers)
        raw_key = record.key
        raw_ts = record.timestamp

        return StreamMessage(
            stream=record.topic,
            id=f"{record.topic}:{record.partition}:{record.offset}",
            payload=self.codec.decode_value(record.value),
            type=message_type,
            key=raw_key.decode("utf-8") if isinstance(raw_key, bytes) else None,
            timestamp=(
                datetime.fromtimestamp(raw_ts / 1000, tz=timezone.utc)
                if raw_ts
                else None
            ),
            partition=record.partition,
            offset=record.offset,
            headers=headers,
        )

    # ....................... #

    async def read(
        self,
        group: str,
        consumer: str,
        topics: Sequence[str],
        *,
        limit: int | None = None,
        timeout: timedelta | None = None,
    ) -> list[StreamMessage[M]]:
        physical = await self._physical_topics(topics)
        kafka_consumer = await self.client.get_consumer(
            group=group,
            member=consumer,
            topics=physical,
            auto_offset_reset=self.auto_offset_reset,
            max_poll_records=self.max_poll_records,
        )
        self._cell.by_group[group] = kafka_consumer

        timeout_ms = (
            int(timeout.total_seconds() * 1000)
            if timeout is not None
            else _DEFAULT_POLL_MS
        )
        batches = await kafka_consumer.getmany(
            timeout_ms=timeout_ms,
            max_records=limit,
        )

        out: list[StreamMessage[M]] = []

        for records in batches.values():
            for record in records:
                out.append(self._to_message(record))

                if limit is not None and len(out) >= limit:
                    return out

        return out

    # ....................... #

    async def tail(
        self,
        group: str,
        consumer: str,
        topics: Sequence[str],
        *,
        timeout: timedelta | None = None,
    ) -> AsyncGenerator[StreamMessage[M]]:
        while True:
            messages = await self.read(group, consumer, topics, timeout=timeout)

            for message in messages:
                yield message

            if not messages and timeout is None:
                await asyncio.sleep(0.05)

    # ....................... #

    async def commit(self, group: str, positions: Sequence[StreamPosition]) -> None:
        kafka_consumer = self._cell.by_group.get(group)

        if kafka_consumer is None or not positions:
            return

        # Kafka commits the NEXT offset to read, so committing processed offset N
        # means committing N + 1; the highest offset per (stream, partition) wins.
        maxima: dict[TopicPartition, int] = {}

        for position in positions:
            tp = TopicPartition(position.stream, position.partition)
            nxt = position.offset + 1

            if nxt > maxima.get(tp, -1):
                maxima[tp] = nxt

        await kafka_consumer.commit(
            {tp: OffsetAndMetadata(offset, "") for tp, offset in maxima.items()}
        )

    # ....................... #

    def capabilities(self) -> CommitStreamGroupCapabilities:
        return CommitStreamGroupCapabilities(
            supports_replay=True,
            supports_transactions=False,
        )
