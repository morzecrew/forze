"""Offset-log (commit sub-model) semantics for the in-memory mock stream adapters.

Mirrors the Kafka commit contract exposed by
:class:`~forze.application.contracts.stream.CommitStreamGroupQueryPort`: a
per-``(group, partition)`` committed cursor gates delivery, only ``commit``
advances it (so a read that is not committed is redelivered), partition
assignment is by ``key`` (per-partition ordering), and ``reset_offsets`` replays.
"""

import asyncio

import pytest
from pydantic import BaseModel

from forze.application.contracts.stream import (
    CommitStreamGroupAware,
    OffsetReset,
    StreamPosition,
)
from forze.application.contracts.stream.specs import StreamSpec
from forze.base.exceptions import CoreException
from forze.base.serialization import PydanticModelCodec
from forze_mock.adapters import (
    MockCommitStreamGroupAdapter,
    MockCommitStreamGroupAdminAdapter,
    MockState,
    MockStreamAdapter,
)

# ----------------------- #


class _Msg(BaseModel):
    v: int


def _adapters() -> tuple[
    MockStreamAdapter[_Msg],
    MockCommitStreamGroupAdapter[_Msg],
    MockCommitStreamGroupAdminAdapter[_Msg],
]:
    st = MockState()
    producer = MockStreamAdapter(
        state=st,
        namespace="orders",
        codec=StreamSpec(name="orders", codec=PydanticModelCodec(model_type=_Msg)).codec,
    )
    query = MockCommitStreamGroupAdapter(stream=producer, state=st, namespace="orders")
    admin = MockCommitStreamGroupAdminAdapter(stream=producer, state=st)
    return producer, query, admin


async def _produce(producer: MockStreamAdapter[_Msg], n: int, *, key: str | None) -> None:
    for i in range(n):
        await producer.append("orders", _Msg(v=i), key=key)


# ....................... #


@pytest.mark.asyncio
async def test_read_populates_typed_position_and_canonical_id() -> None:
    producer, query, admin = _adapters()
    await admin.ensure_topic("orders", partitions=1)
    await admin.ensure_group("g", ["orders"], start=OffsetReset.EARLIEST)
    await _produce(producer, 2, key="k")

    batch = await query.read("g", "c", ["orders"])

    assert [m.offset for m in batch] == [0, 1]
    assert all(m.partition == 0 for m in batch)
    assert batch[0].id == "orders:0:0"
    assert StreamPosition.from_message(batch[1]) == StreamPosition(
        stream="orders", partition=0, offset=1
    )


@pytest.mark.asyncio
async def test_same_key_keeps_partition_and_order() -> None:
    producer, query, admin = _adapters()
    await admin.ensure_topic("orders", partitions=8)
    await admin.ensure_group("g", ["orders"], start=OffsetReset.EARLIEST)
    await _produce(producer, 5, key="same")

    batch = await query.read("g", "c", ["orders"])

    partitions = {m.partition for m in batch}
    assert len(partitions) == 1  # one key → one partition
    assert [m.offset for m in batch] == [0, 1, 2, 3, 4]  # produce order preserved


@pytest.mark.asyncio
async def test_uncommitted_read_is_redelivered() -> None:
    producer, query, admin = _adapters()
    await admin.ensure_group("g", ["orders"], start=OffsetReset.EARLIEST)
    await _produce(producer, 3, key="k")

    first = await query.read("g", "c", ["orders"])
    # Crash before commit: nothing advanced, so re-read sees the same batch.
    second = await query.read("g", "c", ["orders"])

    assert [m.offset for m in first] == [m.offset for m in second] == [0, 1, 2]


@pytest.mark.asyncio
async def test_commit_advances_cursor_high_water_mark() -> None:
    producer, query, admin = _adapters()
    await admin.ensure_group("g", ["orders"], start=OffsetReset.EARLIEST)
    await _produce(producer, 3, key="k")

    batch = await query.read("g", "c", ["orders"])
    # Commit only the middle position; the high-water mark covers 0..1.
    await query.commit("g", [StreamPosition.from_message(batch[1])])

    remaining = await query.read("g", "c", ["orders"])
    assert [m.offset for m in remaining] == [2]


@pytest.mark.asyncio
async def test_latest_start_skips_backlog() -> None:
    producer, query, admin = _adapters()
    await _produce(producer, 3, key="k")
    await admin.ensure_group("g", ["orders"], start=OffsetReset.LATEST)

    assert await query.read("g", "c", ["orders"]) == []

    await _produce(producer, 1, key="k")
    batch = await query.read("g", "c", ["orders"])
    assert [m.offset for m in batch] == [3]


@pytest.mark.asyncio
async def test_reset_offsets_replays_from_earliest() -> None:
    producer, query, admin = _adapters()
    await admin.ensure_group("g", ["orders"], start=OffsetReset.EARLIEST)
    await _produce(producer, 3, key="k")

    batch = await query.read("g", "c", ["orders"])
    await query.commit("g", [StreamPosition.from_message(m) for m in batch])
    assert await query.read("g", "c", ["orders"]) == []

    await admin.reset_offsets("g", "orders", to=OffsetReset.EARLIEST)
    replay = await query.read("g", "c", ["orders"])
    assert [m.offset for m in replay] == [0, 1, 2]


@pytest.mark.asyncio
async def test_reset_offsets_to_explicit_offset() -> None:
    producer, query, admin = _adapters()
    await admin.ensure_group("g", ["orders"], start=OffsetReset.EARLIEST)
    await _produce(producer, 5, key="k")

    await admin.reset_offsets("g", "orders", to=OffsetReset.at_offset(3))
    batch = await query.read("g", "c", ["orders"])
    assert [m.offset for m in batch] == [3, 4]


@pytest.mark.asyncio
async def test_lag_reports_committed_end_and_gap() -> None:
    producer, query, admin = _adapters()
    await admin.ensure_topic("orders", partitions=1)
    await admin.ensure_group("g", ["orders"], start=OffsetReset.EARLIEST)
    await _produce(producer, 4, key="k")

    batch = await query.read("g", "c", ["orders"])
    await query.commit("g", [StreamPosition.from_message(batch[0])])

    lag = await admin.lag("g", "orders")
    (entry,) = [row for row in lag if row.end_offset > 0]
    assert (entry.committed_offset, entry.end_offset, entry.lag) == (1, 4, 3)


@pytest.mark.asyncio
async def test_ensure_topic_rejects_zero_partitions() -> None:
    _producer, _query, admin = _adapters()
    with pytest.raises(CoreException):
        await admin.ensure_topic("orders", partitions=0)


@pytest.mark.asyncio
async def test_query_and_admin_report_capabilities() -> None:
    _producer, query, admin = _adapters()
    assert isinstance(query, CommitStreamGroupAware)
    assert isinstance(admin, CommitStreamGroupAware)
    assert query.capabilities().supports_replay is True
    assert query.capabilities().supports_transactions is False


@pytest.mark.asyncio
async def test_tail_yields_then_idles() -> None:
    producer, query, admin = _adapters()
    await admin.ensure_group("g", ["orders"], start=OffsetReset.EARLIEST)
    await _produce(producer, 2, key="k")

    seen: list[int] = []

    async def _drain() -> None:
        async for msg in query.tail("g", "c", ["orders"]):
            seen.append(msg.offset or 0)
            await query.commit("g", [StreamPosition.from_message(msg)])
            if len(seen) == 2:
                return

    await asyncio.wait_for(_drain(), timeout=2)
    assert seen == [0, 1]
