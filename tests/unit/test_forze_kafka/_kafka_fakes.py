"""Shared fakes for forze_kafka unit tests.

The adapters are a thin translation layer over ``aiokafka``; these fakes let the
unit tests exercise that translation (encode/decode, position ids, commit
offset+1, lag math, reset targets) without a broker. End-to-end at-least-once /
ordering / replay is covered by the integration testcontainer suite and RFC
0007's mock conformance battery.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from types import SimpleNamespace
from typing import Any

from aiokafka.errors import TopicAlreadyExistsError
from aiokafka.structs import (
    OffsetAndMetadata,
    OffsetAndTimestamp,
    TopicPartition,
)
from pydantic import BaseModel

from forze.application.contracts.stream import StreamSpec
from forze.base.serialization import PydanticModelCodec
from forze_kafka.adapters import KafkaStreamCodec
from forze_kafka.kernel.rebalance import KafkaCommitRebalanceListener

# ----------------------- #


class Msg(BaseModel):
    body: str


def make_codec() -> KafkaStreamCodec[Msg]:
    return KafkaStreamCodec(
        payload_codec=StreamSpec(name="s", codec=PydanticModelCodec(model_type=Msg)).codec
    )


# ....................... #


def record(
    topic: str,
    partition: int,
    offset: int,
    value: bytes,
    *,
    key: bytes | None = None,
    timestamp: int = 0,
    headers: Sequence[tuple[str, bytes]] = (),
) -> Any:
    """A duck-typed ``ConsumerRecord`` (only the fields the adapter reads)."""

    return SimpleNamespace(
        topic=topic,
        partition=partition,
        offset=offset,
        value=value,
        key=key,
        timestamp=timestamp,
        headers=list(headers),
    )


# ....................... #


class FakeConsumer:
    """Records commits / assignments and returns canned batch + offset data."""

    def __init__(
        self,
        *,
        batches: dict[TopicPartition, list[Any]] | None = None,
        batch_sequence: list[dict[TopicPartition, list[Any]]] | None = None,
        partitions: dict[str, set[int]] | None = None,
        begins: dict[TopicPartition, int] | None = None,
        ends: dict[TopicPartition, int] | None = None,
        times: dict[TopicPartition, OffsetAndTimestamp | None] | None = None,
        assignment: set[TopicPartition] | None = None,
        seek_error: Exception | None = None,
        assignment_after_seek: set[TopicPartition] | None = None,
    ) -> None:
        self._batches = batches or {}
        self._batch_sequence = batch_sequence
        self._partitions = partitions or {}
        self._begins = begins or {}
        self._ends = ends or {}
        self._times = times or {}
        self._assignment = assignment or set()
        self._seek_error = seek_error
        self._assignment_after_seek = assignment_after_seek
        self.committed: dict[TopicPartition, OffsetAndMetadata] = {}
        self.assigned: list[TopicPartition] = []
        self.sought: list[list[TopicPartition]] = []
        self.stopped = False

    async def getmany(
        self, *, timeout_ms: int = 0, max_records: int | None = None
    ) -> dict[TopicPartition, list[Any]]:
        del timeout_ms, max_records
        if self._batch_sequence:
            # Pop each queued batch, repeating the last (models empty polls → data).
            return (
                self._batch_sequence.pop(0)
                if len(self._batch_sequence) > 1
                else self._batch_sequence[0]
            )
        return self._batches

    async def commit(self, offsets: dict[TopicPartition, OffsetAndMetadata]) -> None:
        self.committed.update(offsets)

    async def topics(self) -> set[str]:
        return set(self._partitions)

    def partitions_for_topic(self, topic: str) -> set[int] | None:
        return self._partitions.get(topic)

    def assign(self, partitions: list[TopicPartition]) -> None:
        self.assigned = list(partitions)

    def assignment(self) -> set[TopicPartition]:
        return set(self._assignment)

    async def seek_to_committed(self, *partitions: TopicPartition) -> None:
        self.sought.append(list(partitions))

        if self._seek_error is not None:
            # A rebalance that lands mid-seek takes the partitions with it; a coordinator
            # blip does not, and the difference is the whole point of the guard.
            if self._assignment_after_seek is not None:
                self._assignment = self._assignment_after_seek

            raise self._seek_error

    async def beginning_offsets(
        self, partitions: list[TopicPartition]
    ) -> dict[TopicPartition, int]:
        return {tp: self._begins.get(tp, 0) for tp in partitions}

    async def end_offsets(self, partitions: list[TopicPartition]) -> dict[TopicPartition, int]:
        return {tp: self._ends.get(tp, 0) for tp in partitions}

    async def offsets_for_times(
        self, timestamps: dict[TopicPartition, int]
    ) -> dict[TopicPartition, OffsetAndTimestamp | None]:
        return {tp: self._times.get(tp) for tp in timestamps}

    async def stop(self) -> None:
        self.stopped = True


# ....................... #


class FakeAdmin:
    """Records created topics; returns preset group offsets + topic partitions."""

    def __init__(
        self,
        *,
        group_offsets: dict[TopicPartition, OffsetAndMetadata] | None = None,
        topic_partitions: dict[str, list[int]] | None = None,
    ) -> None:
        self.created: list[Any] = []
        self._group_offsets = group_offsets or {}
        self._topic_partitions = topic_partitions or {}

    async def create_topics(self, new_topics: list[Any]) -> None:
        for topic in new_topics:
            if any(existing.name == topic.name for existing in self.created):
                raise TopicAlreadyExistsError(topic.name)
            self.created.append(topic)

    async def list_consumer_group_offsets(
        self, group: str
    ) -> dict[TopicPartition, OffsetAndMetadata]:
        del group
        return self._group_offsets

    async def list_topics(self) -> list[str]:
        return [*self._topic_partitions, *(topic.name for topic in self.created)]

    async def describe_topics(self, topics: list[str]) -> list[dict[str, Any]]:
        return [
            {
                "topic": topic,
                "partitions": [{"partition": p} for p in self._topic_partitions.get(topic, [])],
            }
            for topic in topics
        ]


# ....................... #


class FakeKafkaClient:
    """In-memory ``KafkaClientPort`` recording sends and handing back fakes."""

    def __init__(
        self,
        *,
        send_partition: int = 0,
        send_offset: int = 0,
        consumer: FakeConsumer | None = None,
        consumers_by_group: dict[str, FakeConsumer] | None = None,
        consumers_by_member: dict[str, FakeConsumer] | None = None,
        transient: FakeConsumer | None = None,
        admin: FakeAdmin | None = None,
    ) -> None:
        self.sends: list[dict[str, Any]] = []
        self._send_partition = send_partition
        self._send_offset = send_offset
        self._consumer = consumer or FakeConsumer()
        self._consumers_by_group = consumers_by_group or {}
        self._consumers_by_member = consumers_by_member or {}
        self._transient = transient or FakeConsumer()
        self._admin = admin or FakeAdmin()
        self.get_consumer_calls: list[dict[str, Any]] = []
        self.last_listener: KafkaCommitRebalanceListener | None = None
        self.discarded: list[FakeConsumer] = []

    async def close(self) -> None:  # pragma: no cover - not exercised
        return None

    async def health(self) -> tuple[str, bool]:  # pragma: no cover
        return ("Kafka", True)

    async def send(
        self,
        topic: str,
        value: bytes,
        *,
        key: bytes | None = None,
        headers: Sequence[tuple[str, bytes]] | None = None,
        timestamp_ms: int | None = None,
    ) -> Any:
        self.sends.append(
            {
                "topic": topic,
                "value": value,
                "key": key,
                "headers": list(headers or []),
                "timestamp_ms": timestamp_ms,
            }
        )
        return SimpleNamespace(
            topic=topic, partition=self._send_partition, offset=self._send_offset
        )

    async def discard_consumer(self, consumer: Any) -> None:
        self.discarded.append(consumer)

    async def get_consumer(
        self,
        *,
        group: str,
        member: str,
        topics: Sequence[str],
        auto_offset_reset: str | None = None,
        max_poll_records: int | None = None,
        listener: KafkaCommitRebalanceListener | None = None,
    ) -> FakeConsumer:
        self.get_consumer_calls.append(
            {
                "group": group,
                "member": member,
                "topics": list(topics),
                "auto_offset_reset": auto_offset_reset,
                "max_poll_records": max_poll_records,
            }
        )
        if member in self._consumers_by_member:
            consumer = self._consumers_by_member[member]
        else:
            consumer = self._consumers_by_group.get(group, self._consumer)

        # Mirror the real client: bind the live consumer so the listener's
        # assignment seek can act on it.
        self.last_listener = listener
        if isinstance(listener, KafkaCommitRebalanceListener):
            listener.consumer = consumer  # type: ignore[assignment]

        return consumer

    async def new_transient_consumer(self, *, group: str | None = None) -> FakeConsumer:
        del group
        return self._transient

    async def admin(self) -> FakeAdmin:
        return self._admin

    def group_config(self) -> Mapping[str, object]:  # pragma: no cover
        return {}
