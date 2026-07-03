"""Kafka consumer adapter: record→message mapping, commit offset+1, capabilities."""

from datetime import timedelta

from aiokafka.structs import TopicPartition

from forze.application.contracts.stream import (
    CommitStreamGroupCapabilities,
    StreamPosition,
)
from forze_kafka.adapters import KafkaCommitStreamGroupAdapter

from _kafka_fakes import FakeConsumer, FakeKafkaClient, Msg, make_codec, record

# ----------------------- #


def _adapter(client: FakeKafkaClient) -> KafkaCommitStreamGroupAdapter[Msg]:
    return KafkaCommitStreamGroupAdapter(
        client=client,  # type: ignore[arg-type]
        codec=make_codec(),
        namespace="",
        tenant_aware=False,
        tenant_provider=lambda: None,
    )


async def test_read_maps_records_to_messages() -> None:
    codec = make_codec()
    tp = TopicPartition("events", 1)
    records = [
        record(
            "events",
            1,
            0,
            codec.encode_value(Msg(body="a")),
            key=b"k",
            timestamp=1_600_000_000_000,
            headers=[("forze_type", b"evt"), ("forze_event_id", b"e1")],
        ),
        record("events", 1, 1, codec.encode_value(Msg(body="b"))),
    ]
    client = FakeKafkaClient(consumer=FakeConsumer(batches={tp: records}))
    adapter = _adapter(client)

    messages = await adapter.read("g", "m", ["events"])

    assert [m.payload.body for m in messages] == ["a", "b"]
    first = messages[0]
    assert first.partition == 1
    assert first.offset == 0
    assert first.id == "events:1:0"
    assert first.stream == "events"
    assert first.type == "evt"
    assert first.key == "k"
    assert first.headers == {"forze_event_id": "e1"}
    assert client.get_consumer_calls[0]["topics"] == ["events"]
    assert client.get_consumer_calls[0]["member"] == "m"


async def test_read_respects_limit() -> None:
    codec = make_codec()
    tp = TopicPartition("events", 0)
    records = [
        record("events", 0, i, codec.encode_value(Msg(body=str(i)))) for i in range(3)
    ]
    client = FakeKafkaClient(consumer=FakeConsumer(batches={tp: records}))
    adapter = _adapter(client)

    messages = await adapter.read("g", "m", ["events"], limit=2)

    assert len(messages) == 2


async def test_commit_translates_to_next_offset_max_per_partition() -> None:
    consumer = FakeConsumer()
    adapter = _adapter(FakeKafkaClient(consumer=consumer))

    await adapter.read("g", "m", ["events"])  # binds the consumer to the cell
    await adapter.commit(
        "g",
        [
            StreamPosition(stream="events", partition=0, offset=3),
            StreamPosition(stream="events", partition=0, offset=7),
            StreamPosition(
                stream="events", partition=0, offset=1
            ),  # out-of-order, ignored
            StreamPosition(stream="events", partition=1, offset=2),
        ],
    )

    # Highest offset per partition wins; the lower straggler does not regress it.
    assert consumer.committed[TopicPartition("events", 0)].offset == 8
    assert consumer.committed[TopicPartition("events", 1)].offset == 3


async def test_commit_targets_the_group_that_read_not_the_latest() -> None:
    # Regression: interleaved reads across groups must not cross-commit.
    g1 = FakeConsumer()
    g2 = FakeConsumer()
    client = FakeKafkaClient(consumers_by_group={"g1": g1, "g2": g2})
    adapter = _adapter(client)

    await adapter.read("g1", "m", ["events"])
    await adapter.read("g2", "m", ["events"])  # latest read is g2

    await adapter.commit("g1", [StreamPosition(stream="events", partition=0, offset=4)])

    assert g1.committed[TopicPartition("events", 0)].offset == 5
    assert g2.committed == {}  # the g2 consumer must be untouched


async def test_commit_before_read_is_noop() -> None:
    consumer = FakeConsumer()
    adapter = _adapter(FakeKafkaClient(consumer=consumer))

    await adapter.commit("g", [StreamPosition(stream="events", partition=0, offset=1)])

    assert consumer.committed == {}


async def test_commit_empty_positions_noop() -> None:
    consumer = FakeConsumer()
    adapter = _adapter(FakeKafkaClient(consumer=consumer))

    await adapter.read("g", "m", ["events"])
    await adapter.commit("g", [])

    assert consumer.committed == {}


async def test_capabilities() -> None:
    adapter = _adapter(FakeKafkaClient())

    assert adapter.capabilities() == CommitStreamGroupCapabilities(
        supports_replay=True,
        supports_transactions=False,
    )


async def test_tail_yields_messages() -> None:
    codec = make_codec()
    tp = TopicPartition("events", 0)
    client = FakeKafkaClient(
        consumer=FakeConsumer(
            batches={tp: [record("events", 0, 0, codec.encode_value(Msg(body="z")))]}
        )
    )
    adapter = _adapter(client)

    stream = adapter.tail("g", "m", ["events"], timeout=timedelta(seconds=1))
    try:
        first = await anext(stream)
    finally:
        await stream.aclose()

    assert first.payload.body == "z"
    assert first.offset == 0
