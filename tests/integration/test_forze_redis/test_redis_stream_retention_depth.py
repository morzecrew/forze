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
