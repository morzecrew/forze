"""Integration tests for Redis stream consumer-group tail polling."""

from __future__ import annotations

import asyncio
from datetime import timedelta
from uuid import uuid4

import pytest

from forze_redis.adapters import RedisStreamAdapter, RedisStreamGroupAdapter
from forze_redis.kernel.platform.client import RedisClient

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_stream_group_tail_yields_messages(
    redis_stream: RedisStreamAdapter,
    redis_stream_group: RedisStreamGroupAdapter,
    redis_client: RedisClient,
    stream_payload_cls: type,
) -> None:
    stream = f"it:grp_tail:{uuid4().hex[:12]}"
    group = "tail-group"
    consumer = "c1"

    await redis_stream.append(stream, stream_payload_cls(value="first"))
    await redis_stream.append(stream, stream_payload_cls(value="second"))
    await redis_client.xgroup_create(stream, group, id="0-0", mkstream=True)

    agen = redis_stream_group.tail(
        group,
        consumer,
        {stream: ">"},
        timeout=timedelta(seconds=1),
    )
    try:
        m1 = await asyncio.wait_for(agen.__anext__(), timeout=3)
        m2 = await asyncio.wait_for(agen.__anext__(), timeout=3)
    finally:
        await agen.aclose()

    values = {m1.payload.value, m2.payload.value}
    assert values == {"first", "second"}
