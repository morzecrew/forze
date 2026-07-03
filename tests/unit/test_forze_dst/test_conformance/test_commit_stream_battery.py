"""Offset-log (commit sub-model) conformance battery, run against the mock.

The mock-only first leg: it pins the semantics a correct offset-log adapter must
exhibit — at-least-once redelivery on crash-before-commit, per-partition ordering
for same-key messages, replay via offset reset, and exactly-once *effect* once the
inbox dedups redeliveries. The same properties, pointed at a real Kafka backend
over testcontainers, are the differential leg the concrete adapter ships with.

A crash before commit is modeled the way the mock defines it: the committed
cursor only advances on ``commit``, so a read that is not committed is returned
again by the next read (a "restarted" consumer), exactly as a broker redelivers
uncommitted offsets after a rebalance.
"""

from __future__ import annotations

from datetime import timedelta

import pytest
from pydantic import BaseModel

from forze.application.contracts.inbox import InboxSpec
from forze.application.contracts.stream import (
    OffsetReset,
    StreamMessage,
    StreamPosition,
    StreamSpec,
)
from forze.base.serialization import PydanticModelCodec
from forze.testing import context_from_modules
from forze_kits.integrations.consumer import CommitStreamGroupConsumer
from forze_mock import MockDepsModule, MockState
from forze_mock.adapters import (
    MockCommitStreamGroupAdapter,
    MockCommitStreamGroupAdminAdapter,
    MockStreamAdapter,
)

# ----------------------- #


class _Event(BaseModel):
    seq: int


_CODEC = PydanticModelCodec(_Event)
_TOPIC = "events"
_STREAM_SPEC = StreamSpec(name=_TOPIC, codec=_CODEC)
_INBOX_SPEC = InboxSpec(name="inbox")


@pytest.fixture
def state() -> MockState:
    return MockState()


def _producer(state: MockState) -> MockStreamAdapter[_Event]:
    return MockStreamAdapter(state=state, namespace=_TOPIC, codec=_CODEC)


def _query(state: MockState) -> MockCommitStreamGroupAdapter[_Event]:
    return MockCommitStreamGroupAdapter(
        stream=_producer(state), state=state, namespace=_TOPIC
    )


def _admin(state: MockState) -> MockCommitStreamGroupAdminAdapter[_Event]:
    return MockCommitStreamGroupAdminAdapter(stream=_producer(state), state=state)


# ....................... #


@pytest.mark.asyncio
async def test_at_least_once_redelivers_on_crash_before_commit(state: MockState) -> None:
    """Property: an uncommitted read is redelivered (crash before commit loses nothing)."""

    admin, producer, query = _admin(state), _producer(state), _query(state)
    await admin.ensure_group("g", [_TOPIC], start=OffsetReset.EARLIEST)
    for i in range(3):
        await producer.append(_TOPIC, _Event(seq=i), key="k")

    first = await query.read("g", "c", [_TOPIC])
    assert [m.offset for m in first] == [0, 1, 2]

    # Crash before commit: a "restarted" consumer re-reads the same offsets.
    redelivered = await query.read("g", "c2", [_TOPIC])
    assert [m.offset for m in redelivered] == [0, 1, 2]


@pytest.mark.asyncio
async def test_per_partition_ordering_for_same_key(state: MockState) -> None:
    """Property: same-key messages share a partition and keep their produce order."""

    admin, producer, query = _admin(state), _producer(state), _query(state)
    await admin.ensure_topic(_TOPIC, partitions=8)
    await admin.ensure_group("g", [_TOPIC], start=OffsetReset.EARLIEST)
    for i in range(6):
        await producer.append(_TOPIC, _Event(seq=i), key="account-42")

    batch = await query.read("g", "c", [_TOPIC])

    assert len({m.partition for m in batch}) == 1
    assert [m.payload.seq for m in batch] == [0, 1, 2, 3, 4, 5]


@pytest.mark.asyncio
async def test_replay_re_reads_from_reset_offset(state: MockState) -> None:
    """Property: reset_offsets(earliest) replays a fully-committed partition."""

    admin, producer, query = _admin(state), _producer(state), _query(state)
    await admin.ensure_group("g", [_TOPIC], start=OffsetReset.EARLIEST)
    for i in range(3):
        await producer.append(_TOPIC, _Event(seq=i), key="k")

    batch = await query.read("g", "c", [_TOPIC])
    await query.commit("g", [StreamPosition.from_message(m) for m in batch])
    assert await query.read("g", "c", [_TOPIC]) == []  # fully consumed

    await admin.reset_offsets("g", _TOPIC, to=OffsetReset.EARLIEST)
    replay = await query.read("g", "c", [_TOPIC])
    assert [m.payload.seq for m in replay] == [0, 1, 2]


@pytest.mark.asyncio
async def test_exactly_once_effect_across_crash_before_commit(state: MockState) -> None:
    """Property: the runner + inbox make redeliveries idempotent (effect runs once)."""

    ctx = context_from_modules(MockDepsModule(state=state, strict_tx=True))
    admin, producer = _admin(state), _producer(state)
    await admin.ensure_group("g", [_TOPIC], start=OffsetReset.EARLIEST)
    for i in range(3):
        await producer.append(_TOPIC, _Event(seq=i), key="k")

    effects: list[int] = []

    async def handler(msg: StreamMessage[_Event]) -> None:
        effects.append(msg.payload.seq)

    consumer = CommitStreamGroupConsumer(
        topics=[_TOPIC],
        group="g",
        consumer="c",
        stream_spec=_STREAM_SPEC,
        handler=handler,
        inbox_spec=_INBOX_SPEC,
        tx_route="default",
    )

    processed = await consumer.run(ctx, timeout=timedelta(milliseconds=100))
    assert processed.processed == 3

    # Simulate a crash that lost the offset commit: replay the same offsets.
    await admin.reset_offsets("g", _TOPIC, to=OffsetReset.EARLIEST)
    replayed = await consumer.run(ctx, timeout=timedelta(milliseconds=100))

    assert replayed.duplicates == 3
    assert replayed.processed == 0
    assert effects == [0, 1, 2]  # effect happened exactly once despite redelivery
