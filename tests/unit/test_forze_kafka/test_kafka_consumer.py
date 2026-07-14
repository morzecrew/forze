"""Kafka consumer adapter: record→message mapping, commit offset+1, capabilities."""

from datetime import timedelta

from aiokafka.structs import TopicPartition

from forze.application.contracts.stream import (
    CommitStreamGroupCapabilities,
    StreamPosition,
    UndecodableStreamPayload,
)
from forze_kafka.adapters import KafkaCommitStreamGroupAdapter, KafkaStreamCodec

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


def _batch(
    codec: KafkaStreamCodec[Msg], topic: str, partition: int
) -> dict[TopicPartition, list[object]]:
    return {
        TopicPartition(topic, partition): [
            record(topic, partition, 0, codec.encode_value(Msg(body="x")))
        ]
    }


async def test_commit_translates_to_next_offset_max_per_partition() -> None:
    codec = make_codec()
    tp0 = TopicPartition("events", 0)
    tp1 = TopicPartition("events", 1)
    consumer = FakeConsumer(
        batches={
            tp0: [record("events", 0, 0, codec.encode_value(Msg(body="a")))],
            tp1: [record("events", 1, 0, codec.encode_value(Msg(body="b")))],
        }
    )
    adapter = _adapter(FakeKafkaClient(consumer=consumer))

    await adapter.read("g", "m", ["events"])  # registers partitions 0 and 1
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
    assert consumer.committed[tp0].offset == 8
    assert consumer.committed[tp1].offset == 3


async def test_commit_targets_the_group_that_read_not_the_latest() -> None:
    # Regression: interleaved reads across groups must not cross-commit.
    codec = make_codec()
    tp0 = TopicPartition("events", 0)
    g1 = FakeConsumer(batches=_batch(codec, "events", 0))
    g2 = FakeConsumer(batches=_batch(codec, "events", 0))
    client = FakeKafkaClient(consumers_by_group={"g1": g1, "g2": g2})
    adapter = _adapter(client)

    await adapter.read("g1", "m", ["events"])
    await adapter.read("g2", "m", ["events"])  # latest read is g2

    await adapter.commit("g1", [StreamPosition(stream="events", partition=0, offset=4)])

    assert g1.committed[tp0].offset == 5
    assert g2.committed == {}  # the g2 consumer must be untouched


async def test_commit_routes_to_member_that_read_the_partition() -> None:
    # Regression: same group, two members reading different topics — a commit for
    # topic `a` must go through m1's consumer, never m2's (which would fail on
    # unassigned partitions or advance the wrong member).
    codec = make_codec()
    tp_a = TopicPartition("a", 0)
    m1 = FakeConsumer(batches=_batch(codec, "a", 0))
    m2 = FakeConsumer(batches=_batch(codec, "b", 0))
    client = FakeKafkaClient(consumers_by_member={"m1": m1, "m2": m2})
    adapter = _adapter(client)

    await adapter.read("g", "m1", ["a"])
    await adapter.read("g", "m2", ["b"])  # same group, other member + topic

    await adapter.commit("g", [StreamPosition(stream="a", partition=0, offset=4)])

    assert m1.committed[tp_a].offset == 5
    assert m2.committed == {}  # m2's consumer must not receive a's offsets


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


async def test_poison_record_does_not_crash_read_or_skip_good_records() -> None:
    # BUG 1: a malformed record in the middle of a batch must not raise out of
    # read() (getmany already advanced the pooled position past the whole batch,
    # so a raise would let a later commit skip the unprocessed records). Instead
    # the poison is surfaced as a marker with its real offset, so the runner can
    # commit only up to the last good record before it and pause.
    codec = make_codec()
    tp = TopicPartition("events", 0)
    records = [
        record("events", 0, 0, codec.encode_value(Msg(body="good0"))),
        record("events", 0, 1, b"\xff not valid json \x00"),  # poison
        record("events", 0, 2, codec.encode_value(Msg(body="good2"))),
    ]
    consumer = FakeConsumer(batches={tp: records})
    adapter = _adapter(FakeKafkaClient(consumer=consumer))

    messages = await adapter.read("g", "m", ["events"])  # must not raise

    assert len(messages) == 3
    assert messages[0].payload.body == "good0"
    assert isinstance(messages[1].payload, UndecodableStreamPayload)
    assert messages[1].offset == 1
    assert messages[1].id == "events:0:1"
    assert messages[2].payload.body == "good2"

    # The runner commits only the good record(s) BEFORE the poison, then pauses.
    # Emulate that single commit: offset 0 → next offset 1, so the poison (1) and
    # the good record after it (2) stay UNcommitted — never skipped past.
    await adapter.commit("g", [StreamPosition.from_message(messages[0])])

    assert consumer.committed[tp].offset == 1


async def test_seek_to_committed_rewinds_each_group_consumer() -> None:
    # BUG 1 (abort path): seek_to_committed rewinds every pooled consumer the group
    # read through, so an aborted/paused batch is re-fetched from committed, not
    # skipped by the advanced in-memory position.
    codec = make_codec()
    tp = TopicPartition("events", 0)
    consumer = FakeConsumer(
        batches={tp: [record("events", 0, 0, codec.encode_value(Msg(body="a")))]},
        assignment={tp},
    )
    adapter = _adapter(FakeKafkaClient(consumer=consumer))

    await adapter.read("g", "m", ["events"])  # registers partition routing
    await adapter.seek_to_committed("g", ["events"])

    assert consumer.sought == [[tp]]


async def test_seek_to_committed_skips_consumer_with_no_assignment() -> None:
    # A consumer that lost its assignment (e.g. mid-rebalance) is not sought — the
    # aiokafka call would raise on unassigned partitions.
    codec = make_codec()
    tp = TopicPartition("events", 0)
    consumer = FakeConsumer(
        batches={tp: [record("events", 0, 0, codec.encode_value(Msg(body="a")))]},
        assignment=set(),
    )
    adapter = _adapter(FakeKafkaClient(consumer=consumer))

    await adapter.read("g", "m", ["events"])
    await adapter.seek_to_committed("g", ["events"])

    assert consumer.sought == []


async def test_rebalance_revocation_clears_routing_then_commit_is_noop() -> None:
    # BUG 2: a rebalance between read and commit revokes the partition. The
    # listener clears the stale routing so the later commit is a skip (redelivered +
    # inbox-deduped), never an IllegalStateError on a member that lost the partition.
    codec = make_codec()
    tp = TopicPartition("events", 0)
    consumer = FakeConsumer(
        batches={tp: [record("events", 0, 0, codec.encode_value(Msg(body="a")))]},
        assignment={tp},
    )
    client = FakeKafkaClient(consumer=consumer)
    adapter = _adapter(client)

    await adapter.read("g", "m", ["events"])
    listener = client.last_listener
    assert listener is not None

    await listener.on_partitions_revoked([tp])

    # Routing for the revoked partition is gone → commit routes nowhere (no-op).
    await adapter.commit("g", [StreamPosition(stream="events", partition=0, offset=4)])
    assert consumer.committed == {}


async def test_rebalance_assignment_seeks_to_committed() -> None:
    # BUG 2: on (re)assignment the listener rewinds assigned partitions to committed
    # so a rebalance never resumes from a stale in-memory position.
    codec = make_codec()
    tp = TopicPartition("events", 0)
    consumer = FakeConsumer(
        batches={tp: [record("events", 0, 0, codec.encode_value(Msg(body="a")))]},
        assignment={tp},
    )
    client = FakeKafkaClient(consumer=consumer)
    adapter = _adapter(client)

    await adapter.read("g", "m", ["events"])
    listener = client.last_listener
    assert listener is not None

    await listener.on_partitions_assigned([tp])

    assert consumer.sought == [[tp]]


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


async def test_a_rewind_that_fails_while_still_assigned_discards_the_consumer() -> None:
    """The failure this path used to swallow — and the only one here that loses records.

    ``getmany`` moved the fetch position past the whole batch. A rewind that does not happen
    leaves it there, past the tail nobody processed and nobody committed. If the partitions are
    *still ours* no rebalance listener fires to fix it, and the pooled consumer is reused by the
    very next read — including the supervised in-process restart — which then resumes beyond
    those records and commits past them. They are never handled, and nothing ever says so.

    So the consumer is dropped: the next read builds a fresh one, which starts at committed.
    """

    codec = make_codec()
    tp = TopicPartition("events", 0)
    consumer = FakeConsumer(
        batches={tp: [record("events", 0, 0, codec.encode_value(Msg(body="a")))]},
        assignment={tp},
        seek_error=RuntimeError("coordinator not available"),  # not a rebalance
    )
    client = FakeKafkaClient(consumer=consumer)
    adapter = _adapter(client)

    await adapter.read("g", "m", ["events"])  # registers partition routing
    await adapter.seek_to_committed("g", ["events"])

    assert consumer.sought == [[tp]]  # it did try
    assert client.discarded == [consumer], "a position it could not restore must not be kept"
    assert consumer.stopped is False  # the *client* stops it; the fake only records the eviction


async def test_a_rewind_that_races_a_rebalance_keeps_the_consumer() -> None:
    """The benign case, and the reason the swallow existed at all.

    The partitions were reassigned mid-seek, so they are someone else's now and the
    subscription's ``on_assign`` positions their new owner at committed. Nothing is owed, and
    discarding a healthy pooled consumer over a routine rebalance would be its own bug.
    """

    codec = make_codec()
    tp = TopicPartition("events", 0)
    consumer = FakeConsumer(
        batches={tp: [record("events", 0, 0, codec.encode_value(Msg(body="a")))]},
        assignment={tp},
        seek_error=RuntimeError("not assigned"),
        assignment_after_seek=set(),  # the rebalance took them
    )
    client = FakeKafkaClient(consumer=consumer)
    adapter = _adapter(client)

    await adapter.read("g", "m", ["events"])
    await adapter.seek_to_committed("g", ["events"])

    assert client.discarded == []
