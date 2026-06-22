"""Real-Redis integration for the realtime stream path the gateway consumes.

Validates that a ``RealtimeSignal`` round-trips over a Redis stream consumer
group (the substrate ``StreamGroupSignalSource`` reads) with the tenant/event-id
headers intact and exclusive, acknowledged delivery.
"""

from __future__ import annotations

from uuid import uuid4

import pytest

pytest.importorskip("redis")

from forze.application.contracts.envelope import HEADER_EVENT_ID, HEADER_TENANT_ID
from forze.application.contracts.realtime import Audience, RealtimeSignal
from forze.base.serialization import PydanticModelCodec
from forze_redis.adapters import (
    RedisStreamAdapter,
    RedisStreamCodec,
    RedisStreamGroupAdapter,
)
from forze_redis.kernel.client import RedisClient

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_realtime_signal_consumer_group_round_trip(redis_client: RedisClient) -> None:
    codec = RedisStreamCodec(payload_codec=PydanticModelCodec(RealtimeSignal))
    writer = RedisStreamAdapter(client=redis_client, codec=codec)
    group_q = RedisStreamGroupAdapter(client=redis_client, codec=codec)

    stream = f"it:realtime:{uuid4().hex[:12]}"
    group, consumer = "realtime-gateway", "gw-1"

    signal = RealtimeSignal.of(Audience.topic("chat:1"), "message.new", {"text": "hi"})
    await writer.append(
        stream,
        signal,
        type="message.new",
        key="topic:chat:1",
        headers={HEADER_TENANT_ID: "t-1", HEADER_EVENT_ID: "evt-1"},
    )
    await redis_client.xgroup_create(stream, group, id="0-0", mkstream=True)

    [message] = await group_q.read(group, consumer, {stream: ">"}, limit=10)

    # the signal survives the round trip over real Redis
    assert message.payload == signal
    assert message.payload.audience == Audience.topic("chat:1")
    assert message.type == "message.new"
    assert dict(message.headers)[HEADER_TENANT_ID] == "t-1"
    assert dict(message.headers)[HEADER_EVENT_ID] == "evt-1"

    # acknowledged delivery — nothing left pending
    assert await group_q.ack(group, stream, [message.id]) == 1
    assert await group_q.pending(group, stream) == []
