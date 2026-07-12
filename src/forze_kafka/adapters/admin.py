"""Kafka admin adapter implementing :class:`CommitStreamGroupAdminPort`."""

from __future__ import annotations

from typing import Mapping, Sequence, final
from uuid import UUID

import attrs
from aiokafka import AIOKafkaConsumer
from aiokafka.admin import NewTopic
from aiokafka.errors import TopicAlreadyExistsError
from aiokafka.structs import OffsetAndMetadata, TopicPartition

from forze.application.contracts.stream import (
    CommitStreamGroupAdminPort,
    CommitStreamGroupCapabilities,
    ConsumerLag,
    OffsetReset,
    OffsetResetKind,
)
from forze.application.contracts.tenancy import TenantProviderPort
from forze.base.exceptions import exc

from ..kernel.client import KafkaClientPort
from ..kernel.relation import NamedResourceSpec, resolve_kafka_topic

# ----------------------- #

_SkipSet = set[tuple[str, int]]
"""Set of ``(topic, partition)`` already positioned — skipped by ``ensure_group``."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class KafkaCommitStreamGroupAdminAdapter(CommitStreamGroupAdminPort):
    """Offset-log control plane over ``AIOKafkaAdminClient`` + transient consumers.

    Topic creation goes through the admin client; group positioning, replay
    (``reset_offsets``), and lag use short-lived group members that seek and
    commit, since Kafka offset mutation needs group coordination the bare admin
    client does not expose.
    """

    client: KafkaClientPort
    namespace: NamedResourceSpec
    tenant_aware: bool
    tenant_provider: TenantProviderPort

    # ....................... #

    def _tenant_id(self) -> UUID | None:
        if not self.tenant_aware:
            return None

        tenant = self.tenant_provider()

        return None if tenant is None else tenant.tenant_id

    # ....................... #

    async def _resolve(self, topic: str) -> str:
        return await resolve_kafka_topic(self.namespace, self._tenant_id(), topic)

    # ....................... #

    async def _partition_ids(self, topic: str) -> list[int]:
        """Partition ids for *topic* from cluster metadata (via the admin client).

        Existence is checked against the all-topics listing first: naming a
        missing topic in a metadata request makes a broker with topic
        auto-creation enabled (Redpanda's default; also a default-configured
        Kafka) create it as a side effect, and an admin *read* like ``lag``
        must not mint topics. The reliable partition source after that is
        ``describe_topics`` — a bare consumer's ``partitions_for_topic`` only
        knows a topic it is subscribed to.
        """

        admin = await self.client.admin()

        if topic not in await admin.list_topics():
            return []

        described = await admin.describe_topics([topic])
        by_topic = {entry.get("topic"): entry for entry in described}
        entry = by_topic.get(topic)

        if (
            entry is None
        ):  # pragma: no cover - describe_topics returns an entry per queried topic
            return []

        return sorted(part["partition"] for part in entry.get("partitions", []))

    # ....................... #

    async def ensure_topic(
        self,
        stream: str,
        *,
        partitions: int,
        replication: int = 1,
        config: Mapping[str, str] | None = None,
    ) -> None:
        if partitions < 1:
            raise exc.validation("ensure_topic requires partitions >= 1")

        topic = await self._resolve(stream)
        admin = await self.client.admin()

        new_topic = NewTopic(
            topic,
            num_partitions=partitions,
            replication_factor=replication,
            topic_configs=dict(config) if config else None,
        )

        try:
            await admin.create_topics([new_topic])
        except TopicAlreadyExistsError:
            pass  # idempotent

    # ....................... #

    async def ensure_group(
        self,
        group: str,
        topics: Sequence[str],
        *,
        start: OffsetReset = OffsetReset.LATEST,
    ) -> None:
        admin = await self.client.admin()
        committed = await admin.list_consumer_group_offsets(group)
        already: _SkipSet = {
            (tp.topic, tp.partition)
            for tp, meta in committed.items()
            if meta.offset >= 0
        }

        for logical in topics:
            topic = await self._resolve(logical)
            await self._seek_and_commit(group, topic, start, skip=already)

    # ....................... #

    async def reset_offsets(
        self,
        group: str,
        stream: str,
        *,
        to: OffsetReset,
    ) -> None:
        if (
            not self.capabilities().supports_replay
        ):  # pragma: no cover - Kafka always supports replay
            raise exc.configuration(
                f"Stream {stream!r} backend does not support offset reset / replay.",
                code="stream.replay_unsupported",
            )

        topic = await self._resolve(stream)
        await self._seek_and_commit(group, topic, to, skip=None)

    # ....................... #

    async def lag(
        self,
        group: str,
        stream: str | None = None,
    ) -> list[ConsumerLag]:
        admin = await self.client.admin()
        committed = await admin.list_consumer_group_offsets(group)

        if stream is not None:
            topic = await self._resolve(stream)
            partitions = [
                TopicPartition(topic, p) for p in await self._partition_ids(topic)
            ]
        else:
            partitions = sorted(
                committed.keys(), key=lambda tp: (tp.topic, tp.partition)
            )

        if not partitions:
            return []

        consumer = await self.client.new_transient_consumer()

        try:
            ends = await consumer.end_offsets(partitions)
            begins = await consumer.beginning_offsets(partitions)
        finally:
            await consumer.stop()

        out: list[ConsumerLag] = []

        for tp in partitions:
            meta = committed.get(tp)
            committed_offset = (
                meta.offset
                if meta is not None and meta.offset >= 0
                else begins.get(tp, 0)
            )
            out.append(
                ConsumerLag(
                    stream=tp.topic,
                    partition=tp.partition,
                    committed_offset=committed_offset,
                    end_offset=ends.get(tp, 0),
                )
            )

        return out

    # ....................... #

    async def _seek_and_commit(
        self,
        group: str,
        topic: str,
        target: OffsetReset,
        *,
        skip: _SkipSet | None,
    ) -> None:
        partitions = [
            TopicPartition(topic, p) for p in await self._partition_ids(topic)
        ]

        if skip is not None:
            partitions = [
                tp for tp in partitions if (tp.topic, tp.partition) not in skip
            ]

        if not partitions:
            return

        consumer = await self.client.new_transient_consumer(group=group)

        try:
            consumer.assign(partitions)
            targets = await self._target_offsets(consumer, partitions, target)

            await consumer.commit(
                {tp: OffsetAndMetadata(offset, "") for tp, offset in targets.items()}
            )
        finally:
            await consumer.stop()

    # ....................... #

    async def _target_offsets(
        self,
        consumer: AIOKafkaConsumer,
        partitions: list[TopicPartition],
        target: OffsetReset,
    ) -> dict[TopicPartition, int]:
        if target.kind is OffsetResetKind.EARLIEST:
            return await consumer.beginning_offsets(partitions)

        if target.kind is OffsetResetKind.LATEST:
            return await consumer.end_offsets(partitions)

        if target.kind is OffsetResetKind.OFFSET:
            begins = await consumer.beginning_offsets(partitions)
            ends = await consumer.end_offsets(partitions)
            wanted = target.offset if target.offset is not None else 0

            return {tp: max(begins[tp], min(wanted, ends[tp])) for tp in partitions}

        # TIMESTAMP: first offset at or after the instant, else the partition end.
        when_ms = int(target.timestamp.timestamp() * 1000) if target.timestamp else 0
        found = await consumer.offsets_for_times({tp: when_ms for tp in partitions})
        ends = await consumer.end_offsets(partitions)

        return {
            tp: (found[tp].offset if found.get(tp) is not None else ends[tp])
            for tp in partitions
        }

    # ....................... #

    def capabilities(self) -> CommitStreamGroupCapabilities:
        return CommitStreamGroupCapabilities(
            supports_replay=True,
            supports_transactions=False,
        )
