"""Kafka admin adapter: topic creation, lag, replay/reset target math, ensure_group."""

from datetime import datetime, timezone

import pytest
from aiokafka.structs import OffsetAndMetadata, OffsetAndTimestamp, TopicPartition

from forze.application.contracts.stream import ConsumerLag, OffsetReset
from forze.base.exceptions import CoreException
from forze_kafka.adapters import KafkaCommitStreamGroupAdminAdapter

from _kafka_fakes import FakeAdmin, FakeConsumer, FakeKafkaClient

# ----------------------- #


def _adapter(client: FakeKafkaClient) -> KafkaCommitStreamGroupAdminAdapter:
    return KafkaCommitStreamGroupAdminAdapter(
        client=client,  # type: ignore[arg-type]
        namespace="",
        tenant_aware=False,
        tenant_provider=lambda: None,
    )


async def test_ensure_topic_creates() -> None:
    admin = FakeAdmin()
    adapter = _adapter(FakeKafkaClient(admin=admin))

    await adapter.ensure_topic("events", partitions=3, replication=1)

    assert admin.created[0].name == "events"
    assert admin.created[0].num_partitions == 3


async def test_ensure_topic_is_idempotent() -> None:
    admin = FakeAdmin()
    adapter = _adapter(FakeKafkaClient(admin=admin))

    await adapter.ensure_topic("events", partitions=3)
    await adapter.ensure_topic("events", partitions=3)  # duplicate → swallowed

    assert len(admin.created) == 1


async def test_ensure_topic_rejects_zero_partitions() -> None:
    adapter = _adapter(FakeKafkaClient())

    with pytest.raises(CoreException):
        await adapter.ensure_topic("events", partitions=0)


async def test_lag_uses_committed_and_end() -> None:
    tp = TopicPartition("events", 0)
    admin = FakeAdmin(
        group_offsets={tp: OffsetAndMetadata(4, "")},
        topic_partitions={"events": [0]},
    )
    transient = FakeConsumer(begins={tp: 0}, ends={tp: 10})
    adapter = _adapter(FakeKafkaClient(admin=admin, transient=transient))

    lags = await adapter.lag("g", "events")

    assert lags == [
        ConsumerLag(stream="events", partition=0, committed_offset=4, end_offset=10)
    ]
    assert lags[0].lag == 6
    assert transient.stopped


async def test_lag_uncommitted_falls_back_to_beginning() -> None:
    tp = TopicPartition("events", 0)
    admin = FakeAdmin(group_offsets={}, topic_partitions={"events": [0]})
    transient = FakeConsumer(begins={tp: 2}, ends={tp: 5})
    adapter = _adapter(FakeKafkaClient(admin=admin, transient=transient))

    lags = await adapter.lag("g", "events")

    assert lags[0].committed_offset == 2
    assert lags[0].lag == 3


async def test_lag_all_topics_from_group_offsets() -> None:
    tp = TopicPartition("events", 1)
    admin = FakeAdmin(group_offsets={tp: OffsetAndMetadata(7, "")})
    transient = FakeConsumer(begins={tp: 0}, ends={tp: 9})
    adapter = _adapter(FakeKafkaClient(admin=admin, transient=transient))

    lags = await adapter.lag("g")

    assert lags[0].stream == "events"
    assert lags[0].committed_offset == 7
    assert lags[0].end_offset == 9


async def test_reset_offsets_earliest() -> None:
    tp = TopicPartition("events", 0)
    transient = FakeConsumer(begins={tp: 0}, ends={tp: 9})
    adapter = _adapter(
        FakeKafkaClient(
            admin=FakeAdmin(topic_partitions={"events": [0]}), transient=transient
        )
    )

    await adapter.reset_offsets("g", "events", to=OffsetReset.EARLIEST)

    assert transient.assigned == [tp]
    assert transient.committed[tp].offset == 0


async def test_reset_offsets_latest() -> None:
    tp = TopicPartition("events", 0)
    transient = FakeConsumer(begins={tp: 0}, ends={tp: 9})
    adapter = _adapter(
        FakeKafkaClient(
            admin=FakeAdmin(topic_partitions={"events": [0]}), transient=transient
        )
    )

    await adapter.reset_offsets("g", "events", to=OffsetReset.LATEST)

    assert transient.committed[tp].offset == 9


async def test_reset_offsets_explicit_offset_is_clamped() -> None:
    tp = TopicPartition("events", 0)
    transient = FakeConsumer(begins={tp: 0}, ends={tp: 9})
    adapter = _adapter(
        FakeKafkaClient(
            admin=FakeAdmin(topic_partitions={"events": [0]}), transient=transient
        )
    )

    await adapter.reset_offsets("g", "events", to=OffsetReset.at_offset(100))

    assert transient.committed[tp].offset == 9  # clamped to the end


async def test_reset_offsets_timestamp() -> None:
    tp = TopicPartition("events", 0)
    transient = FakeConsumer(
        ends={tp: 9},
        times={tp: OffsetAndTimestamp(offset=5, timestamp=1)},
    )
    adapter = _adapter(
        FakeKafkaClient(
            admin=FakeAdmin(topic_partitions={"events": [0]}), transient=transient
        )
    )

    await adapter.reset_offsets(
        "g",
        "events",
        to=OffsetReset.at_timestamp(datetime(2021, 1, 1, tzinfo=timezone.utc)),
    )

    assert transient.committed[tp].offset == 5


async def test_reset_offsets_timestamp_no_match_uses_end() -> None:
    tp = TopicPartition("events", 0)
    transient = FakeConsumer(ends={tp: 9}, times={tp: None})
    adapter = _adapter(
        FakeKafkaClient(
            admin=FakeAdmin(topic_partitions={"events": [0]}), transient=transient
        )
    )

    await adapter.reset_offsets(
        "g",
        "events",
        to=OffsetReset.at_timestamp(datetime(2021, 1, 1, tzinfo=timezone.utc)),
    )

    assert transient.committed[tp].offset == 9


async def test_ensure_group_skips_already_committed_partitions() -> None:
    tp0 = TopicPartition("events", 0)
    tp1 = TopicPartition("events", 1)
    admin = FakeAdmin(
        group_offsets={tp0: OffsetAndMetadata(3, "")},
        topic_partitions={"events": [0, 1]},
    )
    transient = FakeConsumer(begins={tp0: 0, tp1: 0}, ends={tp0: 9, tp1: 9})
    adapter = _adapter(FakeKafkaClient(admin=admin, transient=transient))

    await adapter.ensure_group("g", ["events"], start=OffsetReset.EARLIEST)

    assert tp1 in transient.committed
    assert tp0 not in transient.committed  # already positioned → skipped


async def test_capabilities() -> None:
    adapter = _adapter(FakeKafkaClient())

    caps = adapter.capabilities()
    assert caps.supports_replay is True
    assert caps.supports_transactions is False
