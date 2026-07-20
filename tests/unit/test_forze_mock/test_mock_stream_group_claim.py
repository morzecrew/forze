"""Pending-entry recovery on the in-memory mock stream group adapter.

Mirrors the Redis ``XAUTOCLAIM``/``XPENDING`` contract exposed by
:class:`~forze.application.contracts.stream.AckStreamGroupQueryPort`: entries read
but never acked stay pending with their owner, last delivery time, and delivery
count; ``claim`` transfers entries idle past a threshold to a live consumer
(bumping the count and resetting the idle clock) and ``pending`` inspects the
outstanding entries without mutating them.  Idle time follows the bound
:class:`~forze.base.primitives.TimeSource`, so every scenario runs on a frozen
clock.
"""

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta

import pytest
from pydantic import BaseModel

from forze.application.contracts.stream import PendingEntry
from forze.application.contracts.stream.specs import StreamSpec
from forze.base.primitives import FrozenTimeSource, bind_time_source
from forze.base.serialization import PydanticModelCodec
from forze_mock.adapters import (
    MockAckStreamGroupAdapter,
    MockState,
    MockStreamAdapter,
)

# ----------------------- #

_T0 = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)

_IDLE = timedelta(seconds=60)


class _Msg(BaseModel):
    body: str


def _adapters() -> tuple[MockStreamAdapter[_Msg], MockAckStreamGroupAdapter[_Msg]]:
    st = MockState()
    sa = MockStreamAdapter(
        state=st,
        namespace="s",
        codec=StreamSpec(name="s", codec=PydanticModelCodec(model_type=_Msg)).codec,
    )
    sg = MockAckStreamGroupAdapter(stream=sa, state=st, namespace="s")
    return sa, sg


@pytest.fixture
def clock() -> Iterator[FrozenTimeSource]:
    source = FrozenTimeSource(instant=_T0)
    with bind_time_source(source):
        yield source


# ....................... #
# Crash-recovery story


@pytest.mark.asyncio
async def test_claim_recovers_stranded_entries_for_another_consumer(
    clock: FrozenTimeSource,
) -> None:
    """Read without ack -> idle passes -> other consumer claims, processes, acks."""

    sa, sg = _adapters()
    ids = [await sa.append("events", _Msg(body=f"m{i}")) for i in range(3)]

    # Consumer A reads the batch and "crashes" before acking.
    delivered = await sg.read("g", "a", {"events": ">"})
    assert [m.id for m in delivered] == ids

    # Before the idle threshold nothing is claimable.
    assert await sg.claim("g", "b", "events", idle=_IDLE) == []

    clock.instant += _IDLE + timedelta(seconds=1)

    claimed = await sg.claim("g", "b", "events", idle=_IDLE)
    assert [m.id for m in claimed] == ids
    assert [m.payload.body for m in claimed] == ["m0", "m1", "m2"]

    # Claimed entries are redeliveries: still pending, now owned by B with a
    # bumped delivery count and a freshly reset idle clock.
    rows = await sg.pending("g", "events")
    assert rows == [
        PendingEntry(id=i, consumer="b", idle=timedelta(0), delivery_count=2)
        for i in ids
    ]

    # The original consumer's history view no longer owns them; B's does.
    assert await sg.read("g", "a", {"events": "0"}) == []
    assert [m.id for m in await sg.read("g", "b", {"events": "0"})] == ids

    # Ack by the claimer clears the pending list for good.
    assert await sg.ack("g", "events", ids) == 3
    assert await sg.pending("g", "events") == []

    clock.instant += _IDLE * 2
    assert await sg.claim("g", "c", "events", idle=_IDLE) == []


@pytest.mark.asyncio
async def test_claim_resets_idle_clock_and_increments_count_per_claim(
    clock: FrozenTimeSource,
) -> None:
    """Each claim restarts the idle window; a crashed claimer is recoverable too."""

    sa, sg = _adapters()
    (msg_id,) = [await sa.append("events", _Msg(body="m"))]
    await sg.read("g", "a", {"events": ">"})

    clock.instant += _IDLE
    assert [m.id for m in await sg.claim("g", "b", "events", idle=_IDLE)] == [msg_id]

    # Half an idle window later the entry is B's and not yet claimable again.
    clock.instant += _IDLE / 2
    assert await sg.claim("g", "c", "events", idle=_IDLE) == []

    (row,) = await sg.pending("g", "events")
    assert row.consumer == "b"
    assert row.idle == _IDLE / 2
    assert row.delivery_count == 2

    # B "crashes" as well: once a full idle window passes, C recovers it and
    # the delivery counter keeps growing.
    clock.instant += _IDLE / 2
    assert [m.id for m in await sg.claim("g", "c", "events", idle=_IDLE)] == [msg_id]

    (row,) = await sg.pending("g", "events")
    assert row.consumer == "c"
    assert row.delivery_count == 3


# ....................... #
# Threshold and limit


@pytest.mark.asyncio
async def test_claim_respects_idle_threshold_and_limit(
    clock: FrozenTimeSource,
) -> None:
    """Only entries idle past the threshold move; *limit* caps the sweep, oldest first."""

    sa, sg = _adapters()
    old_ids = [await sa.append("events", _Msg(body=f"old{i}")) for i in range(2)]
    await sg.read("g", "a", {"events": ">"})

    # A younger pending entry, delivered half an idle window later.
    clock.instant += _IDLE / 2
    young_id = await sa.append("events", _Msg(body="young"))
    await sg.read("g", "a", {"events": ">"})

    clock.instant += _IDLE / 2

    # Only the old entries qualify; limit=1 claims the oldest one only.
    first = await sg.claim("g", "b", "events", idle=_IDLE, limit=1)
    assert [m.id for m in first] == [old_ids[0]]

    second = await sg.claim("g", "b", "events", idle=_IDLE)
    assert [m.id for m in second] == [old_ids[1]]

    # The young entry stayed with A, untouched bookkeeping included.
    rows = {row.id: row for row in await sg.pending("g", "events")}
    assert rows[young_id].consumer == "a"
    assert rows[young_id].delivery_count == 1
    assert rows[young_id].idle == _IDLE / 2


@pytest.mark.asyncio
async def test_claim_with_exact_idle_boundary_claims_entry(
    clock: FrozenTimeSource,
) -> None:
    """XAUTOCLAIM semantics: an entry idle exactly *idle* is claimable (>=)."""

    sa, sg = _adapters()
    msg_id = await sa.append("events", _Msg(body="m"))
    await sg.read("g", "a", {"events": ">"})

    clock.instant += _IDLE
    assert [m.id for m in await sg.claim("g", "b", "events", idle=_IDLE)] == [msg_id]


@pytest.mark.asyncio
async def test_claim_ignores_acked_and_undelivered_entries(
    clock: FrozenTimeSource,
) -> None:
    """Acked entries and entries never delivered to the group are not claimable."""

    sa, sg = _adapters()
    read_ids = [await sa.append("events", _Msg(body=f"m{i}")) for i in range(2)]
    await sg.read("g", "a", {"events": ">"})
    await sg.ack("g", "events", [read_ids[0]])

    # Appended after the group read: not delivered, hence never pending.
    await sa.append("events", _Msg(body="unread"))

    clock.instant += _IDLE
    claimed = await sg.claim("g", "b", "events", idle=_IDLE)
    assert [m.id for m in claimed] == [read_ids[1]]


# ....................... #
# Pending inspection


@pytest.mark.asyncio
async def test_pending_inspection_matrix(clock: FrozenTimeSource) -> None:
    """Owners, computed idle, delivery counts, id order, and limit."""

    sa, sg = _adapters()

    # Empty group: nothing pending.
    assert await sg.pending("g", "events") == []

    a_ids = [await sa.append("events", _Msg(body=f"a{i}")) for i in range(2)]
    await sg.read("g", "a", {"events": ">"})

    clock.instant += timedelta(seconds=10)
    b_id = await sa.append("events", _Msg(body="b0"))
    await sg.read("g", "b", {"events": ">"})

    clock.instant += timedelta(seconds=5)

    rows = await sg.pending("g", "events")
    assert rows == [
        PendingEntry(
            id=a_ids[0],
            consumer="a",
            idle=timedelta(seconds=15),
            delivery_count=1,
        ),
        PendingEntry(
            id=a_ids[1],
            consumer="a",
            idle=timedelta(seconds=15),
            delivery_count=1,
        ),
        PendingEntry(
            id=b_id,
            consumer="b",
            idle=timedelta(seconds=5),
            delivery_count=1,
        ),
    ]

    # Limit returns the oldest entries first.
    limited = await sg.pending("g", "events", limit=2)
    assert [row.id for row in limited] == a_ids

    # Inspection is read-only: nothing changed.
    assert await sg.pending("g", "events") == rows

    # Ack removes entries from the view; the rest keep aging.
    await sg.ack("g", "events", [a_ids[0]])
    clock.instant += timedelta(seconds=5)

    rows = await sg.pending("g", "events")
    assert [(row.id, row.idle) for row in rows] == [
        (a_ids[1], timedelta(seconds=20)),
        (b_id, timedelta(seconds=10)),
    ]


@pytest.mark.asyncio
async def test_pending_and_claim_are_scoped_per_group(
    clock: FrozenTimeSource,
) -> None:
    """Recovery in one group leaves another group's pending list untouched."""

    sa, sg = _adapters()
    ids = [await sa.append("events", _Msg(body=f"m{i}")) for i in range(2)]
    await sg.read("g1", "a", {"events": ">"})
    await sg.read("g2", "a", {"events": ">"})

    clock.instant += _IDLE
    claimed = await sg.claim("g1", "b", "events", idle=_IDLE)
    assert [m.id for m in claimed] == ids

    g2_rows = await sg.pending("g2", "events")
    assert [(row.consumer, row.delivery_count) for row in g2_rows] == [("a", 1)] * 2
