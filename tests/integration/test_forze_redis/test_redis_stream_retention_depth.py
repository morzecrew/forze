"""Real-Redis legs for the stream retention cap and the ack-group depth surface.

# covers: RedisStreamAdapter.max_entries (XADD MAXLEN ~), RedisStreamGroupAdminAdapter.depth

Retention is approximate (``~`` trims whole macro nodes), so the retention assertion is
about the *mechanism* — the cap demonstrably bounds the stream well below what was
appended — not an exact length. Depth is exact where Redis is exact: backlog from
``XINFO GROUPS`` lag, pending from the PEL, oldest idle from ``XPENDING``.
"""

from __future__ import annotations

import pytest
from pydantic import BaseModel

from forze.base.exceptions import CoreException
from forze.base.serialization import PydanticModelCodec
from forze_redis import RedisClient
from forze_redis.adapters import (
    RedisStreamAdapter,
    RedisStreamCodec,
    RedisStreamGroupAdapter,
    RedisStreamGroupAdminAdapter,
)

# ----------------------- #

pytestmark = pytest.mark.asyncio


class _Payload(BaseModel):
    value: str


def _codec() -> RedisStreamCodec[_Payload]:
    return RedisStreamCodec(payload_codec=PydanticModelCodec(_Payload))


# ----------------------- #


async def test_retention_cap_bounds_the_stream(redis_client: RedisClient) -> None:
    stream = "it-retention"
    capped = RedisStreamAdapter(client=redis_client, codec=_codec(), max_entries=100)

    for i in range(1000):
        await capped.append(stream, _Payload(value=str(i)))

    length = await redis_client.xlen(stream)

    # Approximate trimming: the observed length may exceed the cap by up to a macro
    # node, but a thousand appends against a cap of 100 must not survive intact.
    assert length < 500
    assert length >= 100

    # And the survivors are the newest — retention evicts from the head.
    messages = await capped.read({stream: "0"}, limit=1)
    assert int(messages[0].payload.value) > 0


async def test_uncapped_stream_keeps_everything(redis_client: RedisClient) -> None:
    stream = "it-retention-uncapped"
    adapter = RedisStreamAdapter(client=redis_client, codec=_codec())

    for i in range(300):
        await adapter.append(stream, _Payload(value=str(i)))

    assert await redis_client.xlen(stream) == 300


# ----------------------- #


async def test_depth_reports_backlog_pending_and_rest(redis_client: RedisClient) -> None:
    stream = "it-depth"
    group = "gw"
    codec = _codec()
    adapter = RedisStreamAdapter(client=redis_client, codec=codec)
    reader = RedisStreamGroupAdapter(client=redis_client, codec=codec)
    admin = RedisStreamGroupAdminAdapter(client=redis_client)

    await admin.ensure_group(group, stream, start_id="0")

    for i in range(3):
        await adapter.append(stream, _Payload(value=str(i)))

    undelivered = await admin.depth(group, stream)
    assert undelivered.backlog == 3
    assert undelivered.pending == 0
    assert undelivered.oldest_pending_idle is None
    assert not undelivered.at_rest

    delivered = await reader.read(group, "c1", {stream: ">"})
    assert len(delivered) == 3

    in_flight = await admin.depth(group, stream)
    assert in_flight.backlog == 0
    assert in_flight.pending == 3
    assert in_flight.oldest_pending_idle is not None
    assert not in_flight.at_rest

    await reader.ack(group=group, stream=stream, ids=[m.id for m in delivered])

    at_rest = await admin.depth(group, stream)
    assert at_rest.backlog == 0
    assert at_rest.pending == 0
    assert at_rest.at_rest


async def test_depth_on_missing_group_raises_not_found(redis_client: RedisClient) -> None:
    admin = RedisStreamGroupAdminAdapter(client=redis_client)
    other = RedisStreamGroupAdminAdapter(client=redis_client)

    await other.ensure_group("exists", "it-depth-missing", start_id="0")

    with pytest.raises(CoreException) as caught:
        await admin.depth("never-created", "it-depth-missing")

    assert caught.value.code == "stream_group_not_found"


# ----------------------- #
# trim_acknowledged — the group-floor sweep over real XINFO/XPENDING/XTRIM MINID


async def test_trim_acknowledged_removes_only_the_acked_prefix(
    redis_client: RedisClient,
) -> None:
    stream = "it-trim-floor"
    codec = _codec()
    adapter = RedisStreamAdapter(client=redis_client, codec=codec)
    reader = RedisStreamGroupAdapter(client=redis_client, codec=codec)
    admin = RedisStreamGroupAdminAdapter(client=redis_client)

    await admin.ensure_group("gw", stream, start_id="0")
    for i in range(5):
        await adapter.append(stream, _Payload(value=str(i)))

    delivered = await reader.read("gw", "c1", {stream: ">"})
    assert len(delivered) == 5
    await reader.ack(group="gw", stream=stream, ids=[m.id for m in delivered[:2]])

    # the two acked entries go; the three pending ones hold the floor
    assert await admin.trim_acknowledged(stream) == 2
    assert await redis_client.xlen(stream) == 3

    # acking the rest moves the floor past everything delivered
    await reader.ack(group="gw", stream=stream, ids=[m.id for m in delivered[2:]])
    assert await admin.trim_acknowledged(stream) == 3
    assert await redis_client.xlen(stream) == 0

    # and the pending list survived intact through both trims
    assert await reader.pending("gw", stream) == []


async def test_trim_acknowledged_is_held_by_the_slowest_group(
    redis_client: RedisClient,
) -> None:
    stream = "it-trim-slowest"
    codec = _codec()
    adapter = RedisStreamAdapter(client=redis_client, codec=codec)
    reader = RedisStreamGroupAdapter(client=redis_client, codec=codec)
    admin = RedisStreamGroupAdminAdapter(client=redis_client)

    await admin.ensure_group("fast", stream, start_id="0")
    await admin.ensure_group("slow", stream, start_id="0")
    for i in range(4):
        await adapter.append(stream, _Payload(value=str(i)))

    delivered = await reader.read("fast", "c1", {stream: ">"})
    await reader.ack(group="fast", stream=stream, ids=[m.id for m in delivered])

    # the idle group's undelivered backlog holds the floor at zero
    assert await admin.trim_acknowledged(stream) == 0
    assert await redis_client.xlen(stream) == 4


async def test_trim_acknowledged_without_groups_trims_nothing(
    redis_client: RedisClient,
) -> None:
    stream = "it-trim-groupless"
    adapter = RedisStreamAdapter(client=redis_client, codec=_codec())

    for i in range(3):
        await adapter.append(stream, _Payload(value=str(i)))

    admin = RedisStreamGroupAdminAdapter(client=redis_client)
    assert await admin.trim_acknowledged(stream) == 0
    assert await redis_client.xlen(stream) == 3
