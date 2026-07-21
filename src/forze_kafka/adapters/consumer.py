"""Kafka consumer adapter implementing :class:`CommitStreamGroupQueryPort`."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, Sequence
from datetime import UTC, datetime, timedelta
from functools import partial
from typing import cast, final
from uuid import UUID

import attrs
from aiokafka import AIOKafkaConsumer
from aiokafka.structs import ConsumerRecord, OffsetAndMetadata, TopicPartition

from forze.application.contracts.stream import (
    CommitStreamGroupCapabilities,
    CommitStreamGroupQueryPort,
    StreamMessage,
    StreamPosition,
    UndecodableStreamPayload,
)
from forze.application.contracts.tenancy import TenantProviderPort
from forze.base.logging import Logger

from .._logging import ForzeKafkaLogger
from ..kernel.client import KafkaClientPort
from ..kernel.rebalance import KafkaCommitRebalanceListener
from ..kernel.relation import NamedResourceSpec, resolve_kafka_topic
from .codecs import KafkaStreamCodec

# ----------------------- #

logger = Logger(ForzeKafkaLogger.ADAPTERS)
"""Kafka adapters logger."""

_DEFAULT_POLL_MS = 1_000
"""``getmany`` wait when the caller passes no timeout (the runner polls anyway)."""


# ....................... #


@attrs.define(slots=True, kw_only=True)
class _ConsumerCell:
    """Maps each read ``(group, topic, partition)`` to the consumer that read it.

    The adapter is frozen; this cell records, per delivered partition, the exact
    pooled consumer a ``read`` used, so ``commit`` — which receives only the
    group and positions, not the member/topics the client pools by — routes each
    offset back to the consumer actually assigned that partition. A partition is
    assigned to one group member at a time, so the key is unambiguous even when
    several members of the same group read different topics/partitions."""

    by_partition: dict[tuple[str, str, int | None], AIOKafkaConsumer] = attrs.field(factory=dict)


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

        return [await resolve_kafka_topic(self.namespace, tenant_id, topic) for topic in topics]

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
            timestamp=(datetime.fromtimestamp(raw_ts / 1000, tz=UTC) if raw_ts else None),
            partition=record.partition,
            offset=record.offset,
            headers=headers,
        )

    # ....................... #

    def _poison_message(
        self, record: ConsumerRecord[bytes, bytes], error: Exception
    ) -> StreamMessage[M]:
        """Wrap a record whose value/headers could not be decoded as a poison marker.

        The record's ``(partition, offset)`` / ``id`` are preserved (read straight
        off the native record, not the codec), so the runner can pause-and-alert and
        leave the offset uncommitted for redelivery — never skip it. The payload is a
        :class:`UndecodableStreamPayload`, not the model ``M``.

        Headers and type are re-decoded best-effort and carried on the marker: the
        dominant failure is a *value* codec rejection, where the native headers stayed
        perfectly decodable, and a forwarded sealed envelope needs the ids its AAD binds
        to (``forze_event_id``, tenant, correlation) to be re-openable in the DLQ. Header
        decoding is defensive — a marker must never raise out of the poison path — so if
        the headers are themselves the malformed part they simply fall back to empty.
        """

        raw = record.value if isinstance(record.value, bytes) else b""

        try:
            headers, message_type = self.codec.decode_headers(record.headers)

        except Exception:
            headers, message_type = {}, None

        return StreamMessage(
            stream=record.topic,
            id=f"{record.topic}:{record.partition}:{record.offset}",
            payload=cast(M, UndecodableStreamPayload(raw=raw, error=str(error))),
            type=message_type,
            partition=record.partition,
            offset=record.offset,
            headers=headers,
        )

    # ....................... #

    def _forget_partitions(self, group: str, revoked: Sequence[TopicPartition]) -> None:
        """Drop revoked partitions' commit routing (a rebalance took them away)."""

        for tp in revoked:
            self._cell.by_partition.pop((group, tp.topic, tp.partition), None)

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
            # On a rebalance: drop revoked partitions' routing (so a commit for one
            # doesn't raise on a member that lost it) and rewind assigned ones to
            # committed. Fresh per read; only the first read's listener is live (the
            # consumer keeps the one it was subscribed with), and the closure's cell
            # is stable across reads (the adapter is frozen).
            listener=KafkaCommitRebalanceListener(
                on_revoke=partial(self._forget_partitions, group)
            ),
        )
        timeout_ms = (
            int(timeout.total_seconds() * 1000) if timeout is not None else _DEFAULT_POLL_MS
        )
        batches = await kafka_consumer.getmany(
            timeout_ms=timeout_ms,
            max_records=limit,
        )

        out: list[StreamMessage[M]] = []

        for records in batches.values():
            for record in records:
                try:
                    message = self._to_message(record)

                except Exception as error:
                    # A malformed record must NOT raise out of read(): getmany has
                    # already advanced the pooled consumer past the whole batch, so a
                    # raise here would let a later successful commit skip the
                    # unprocessed records (silent loss on restart). Surface a poison
                    # marker instead — the runner pauses-and-alerts and rewinds to
                    # committed, leaving the offset uncommitted for redelivery.
                    logger.warning(
                        "Kafka read: undecodable record at %s:%s:%s; surfacing poison "
                        "marker (offset left uncommitted for the runner to pause on)",
                        record.topic,
                        record.partition,
                        record.offset,
                        exc_info=True,
                    )
                    message = self._poison_message(record, error)

                # Remember which consumer delivered this partition so its commit
                # routes to the right assigned member.
                self._cell.by_partition[(group, message.stream, message.partition)] = kafka_consumer
                out.append(message)

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
        if not positions:
            return

        # Kafka commits the NEXT offset to read, so committing processed offset N
        # means committing N + 1. Route each position to the consumer that read
        # its partition and take the highest offset per (consumer, partition).
        routed: dict[AIOKafkaConsumer, dict[TopicPartition, int]] = {}

        for position in positions:
            consumer = self._cell.by_partition.get((group, position.stream, position.partition))

            if consumer is None:
                continue  # not read through this adapter — nothing to commit on

            maxima = routed.setdefault(consumer, {})
            tp = TopicPartition(position.stream, position.partition)
            nxt = position.offset + 1

            if nxt > maxima.get(tp, -1):
                maxima[tp] = nxt

        for consumer, maxima in routed.items():
            await consumer.commit(
                {tp: OffsetAndMetadata(offset, "") for tp, offset in maxima.items()}
            )

    # ....................... #

    async def seek_to_committed(self, group: str, topics: Sequence[str]) -> None:
        del topics  # every pooled consumer for the group is rewound wholesale

        # Rewind each pooled consumer this group read through back to its committed
        # offset. ``getmany`` advanced the in-memory position past the whole batch;
        # on an abort/pause (poison, crash) that advance must be undone or the next
        # read — including a supervised in-process restart reusing this same pooled
        # consumer — would resume PAST the uncommitted records and skip them.
        seen: set[int] = set()

        for (member_group, _topic, _partition), consumer in list(self._cell.by_partition.items()):
            if member_group != group or id(consumer) in seen:
                continue

            seen.add(id(consumer))
            assigned = consumer.assignment()

            if not assigned:
                continue

            try:
                await consumer.seek_to_committed(*assigned)

            except Exception:
                await self._rewind_failed(group, consumer, assigned)

    # ....................... #

    async def _rewind_failed(
        self,
        group: str,
        consumer: AIOKafkaConsumer,
        attempted: set[TopicPartition],
    ) -> None:
        """A rewind that did not happen — the one failure here that silently loses records.

        **A rebalance mid-seek is benign**, and that is the only case this used to assume. The
        partitions are someone else's now, and the subscription's ``on_assign`` positions whoever
        gets them at the committed offset. Nothing is owed.

        **Anything else is not**, and treating it as such is how records disappear. A coordinator
        timeout or a broker blip while the partitions are *still ours* leaves this consumer's
        fetch position where ``getmany`` put it: past the whole batch, and so past the tail we
        never processed and never committed. No listener fires, because nothing was reassigned.
        Reuse the consumer — which a supervised in-process restart does, since it is pooled — and
        the next read resumes beyond those records and commits past them. They are never handled,
        and nothing ever says so.

        So a position we could not restore is a position we must not keep: drop the consumer. The
        next read builds a fresh one, and a fresh consumer starts where the group committed.
        """

        if not attempted & consumer.assignment():
            logger.warning(
                "Kafka seek_to_committed for group %s raced a rebalance; its partitions are "
                "reassigned and their new owner starts at the committed offset",
                group,
                exc_info=True,
            )
            return

        logger.error(
            "Kafka seek_to_committed failed for group %s while it still holds the partitions, so "
            "its fetch position is still past records nobody processed; discarding the pooled "
            "consumer so the next read starts from the committed offset instead of skipping them",
            group,
            exc_info=True,
        )

        for key, pooled in list(self._cell.by_partition.items()):
            if pooled is consumer:
                del self._cell.by_partition[key]

        await self.client.discard_consumer(consumer)

    # ....................... #

    def capabilities(self) -> CommitStreamGroupCapabilities:
        return CommitStreamGroupCapabilities(
            supports_replay=True,
            supports_transactions=False,
        )
