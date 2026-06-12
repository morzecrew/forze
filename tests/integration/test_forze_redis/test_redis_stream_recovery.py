"""Integration tests for stream-group pending-entry recovery (XAUTOCLAIM/XPENDING).

Runs the crash-recovery story end-to-end against a real server: a consumer
reads a batch and dies before acking, the entries strand in the group's
pending-entries list, and a second consumer claims them after the idle
threshold, processes, and acks.  Also exercises the XAUTOCLAIM cursor loop and
XPENDING pagination across multiple pages.
"""

import asyncio
from datetime import timedelta
from uuid import uuid4

import pytest

from forze_redis.adapters import RedisStreamAdapter, RedisStreamGroupAdapter
from forze_redis.kernel.client import RedisClient

# ----------------------- #

_IDLE = timedelta(milliseconds=100)
"""Claim threshold used by the recovery tests."""

_IDLE_SLEEP = 0.4
"""Real-clock sleep (seconds) comfortably exceeding :data:`_IDLE`."""


def _stream_name() -> str:
    return f"it:stream:{uuid4().hex[:12]}"


# ....................... #


@pytest.mark.asyncio
async def test_crash_recovery_story_end_to_end(
    redis_stream: RedisStreamAdapter,
    redis_stream_group: RedisStreamGroupAdapter,
    redis_client: RedisClient,
    stream_payload_cls,
) -> None:
    """A reads 3 and acks 1, "crashes"; B claims the 2 stranded entries and acks."""

    stream = _stream_name()
    group = "recovery-group"

    ids = [
        await redis_stream.append(stream, stream_payload_cls(value=f"v{i}"))
        for i in range(3)
    ]
    await redis_client.xgroup_create(stream, group, id="0-0", mkstream=True)

    # Consumer A reads the whole batch, acks only the first entry, then dies.
    delivered = await redis_stream_group.read(group, "a", {stream: ">"}, limit=10)
    assert [m.id for m in delivered] == ids
    assert await redis_stream_group.ack(group, stream, [ids[0]]) == 1

    # Intermediate state: the two unacked entries are pending for A,
    # delivered exactly once.
    rows = await redis_stream_group.pending(group, stream)
    assert [(r.id, r.consumer, r.delivery_count) for r in rows] == [
        (ids[1], "a", 1),
        (ids[2], "a", 1),
    ]

    await asyncio.sleep(_IDLE_SLEEP)

    # Stranded past the idle threshold: B claims exactly the unacked two.
    claimed = await redis_stream_group.claim(group, "b", stream, idle=_IDLE)
    assert [m.id for m in claimed] == [ids[1], ids[2]]
    assert [m.payload.value for m in claimed] == ["v1", "v2"]
    assert all(m.stream == stream for m in claimed)

    # The claim is a redelivery: still pending, now owned by B with the
    # delivery counter bumped and the idle clock reset.
    rows = await redis_stream_group.pending(group, stream)
    assert [(r.id, r.consumer, r.delivery_count) for r in rows] == [
        (ids[1], "b", 2),
        (ids[2], "b", 2),
    ]
    assert all(r.idle < timedelta(seconds=_IDLE_SLEEP) for r in rows)

    # B processes and acks: the group's pending list drains completely.
    assert await redis_stream_group.ack(group, stream, [ids[1], ids[2]]) == 2
    assert await redis_stream_group.pending(group, stream) == []
    assert await redis_stream_group.claim(group, "c", stream, idle=timedelta(0)) == []


@pytest.mark.asyncio
async def test_claim_leaves_entries_younger_than_idle_untouched(
    redis_stream: RedisStreamAdapter,
    redis_stream_group: RedisStreamGroupAdapter,
    redis_client: RedisClient,
    stream_payload_cls,
) -> None:
    stream = _stream_name()
    group = "young-group"

    await redis_stream.append(stream, stream_payload_cls(value="fresh"))
    await redis_client.xgroup_create(stream, group, id="0-0", mkstream=True)
    await redis_stream_group.read(group, "a", {stream: ">"}, limit=10)

    assert await redis_stream_group.claim(group, "b", stream, idle=timedelta(hours=1)) == []

    (row,) = await redis_stream_group.pending(group, stream)
    assert (row.consumer, row.delivery_count) == ("a", 1)


@pytest.mark.asyncio
async def test_claim_respects_limit(
    redis_stream: RedisStreamAdapter,
    redis_stream_group: RedisStreamGroupAdapter,
    redis_client: RedisClient,
    stream_payload_cls,
) -> None:
    stream = _stream_name()
    group = "limit-group"

    ids = [
        await redis_stream.append(stream, stream_payload_cls(value=f"v{i}"))
        for i in range(5)
    ]
    await redis_client.xgroup_create(stream, group, id="0-0", mkstream=True)
    await redis_stream_group.read(group, "a", {stream: ">"}, limit=10)

    await asyncio.sleep(_IDLE_SLEEP)

    claimed = await redis_stream_group.claim(group, "b", stream, idle=_IDLE, limit=2)
    assert [m.id for m in claimed] == ids[:2]

    owners = {r.id: r.consumer for r in await redis_stream_group.pending(group, stream)}
    assert [owners[i] for i in ids] == ["b", "b", "a", "a", "a"]


@pytest.mark.asyncio
async def test_recovery_sweeps_span_multiple_pages(
    redis_stream: RedisStreamAdapter,
    redis_stream_group: RedisStreamGroupAdapter,
    redis_client: RedisClient,
    stream_payload_cls,
) -> None:
    """130 pending entries: XAUTOCLAIM loops its cursor and XPENDING pages by 100."""

    stream = _stream_name()
    group = "paged-group"
    total = 130  # above both the XAUTOCLAIM server default and the XPENDING page

    ids = [
        await redis_stream.append(stream, stream_payload_cls(value=f"v{i}"))
        for i in range(total)
    ]
    await redis_client.xgroup_create(stream, group, id="0-0", mkstream=True)

    delivered = await redis_stream_group.read(group, "a", {stream: ">"})
    assert len(delivered) == total

    # pending() pagination: a limit spanning multiple pages, then the full list.
    spanning = await redis_stream_group.pending(group, stream, limit=110)
    assert [r.id for r in spanning] == ids[:110]

    rows = await redis_stream_group.pending(group, stream)
    assert [r.id for r in rows] == ids
    assert {r.consumer for r in rows} == {"a"}

    await asyncio.sleep(_IDLE_SLEEP)

    # Unbounded claim: the server pages XAUTOCLAIM (default count 100), so the
    # adapter must follow the cursor across pages to recover everything.
    claimed = await redis_stream_group.claim(group, "b", stream, idle=_IDLE)
    assert [m.id for m in claimed] == ids

    rows = await redis_stream_group.pending(group, stream)
    assert len(rows) == total
    assert {(r.consumer, r.delivery_count) for r in rows} == {("b", 2)}

    assert await redis_stream_group.ack(group, stream, ids) == total
    assert await redis_stream_group.pending(group, stream) == []
