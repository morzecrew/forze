"""Integration tests for RedisStreamAdapter and RedisStreamGroupAdapter."""

from uuid import uuid4

import pytest

from forze_redis.adapters import RedisStreamAdapter, RedisStreamGroupAdapter
from forze_redis.kernel.platform.client import RedisClient


def _stream_name() -> str:
    return f"it:stream:{uuid4().hex[:12]}"


@pytest.mark.asyncio
async def test_stream_append_and_read(
    redis_stream: RedisStreamAdapter,
    stream_payload_cls,
) -> None:
    """append writes message; read retrieves it."""
    stream = _stream_name()
    msg_id = await redis_stream.append(stream, stream_payload_cls(value="hello"))
    assert msg_id
    assert "-" in msg_id  # Redis stream ID format

    messages = await redis_stream.read({stream: "0"}, limit=10)
    assert len(messages) == 1
    assert messages[0]["stream"] == stream
    assert messages[0]["id"] == msg_id
    assert messages[0]["payload"].value == "hello"


@pytest.mark.asyncio
async def test_stream_read_with_cursor(
    redis_stream: RedisStreamAdapter,
    stream_payload_cls,
) -> None:
    """read respects stream cursor (last_id) and limit."""
    stream = _stream_name()
    await redis_stream.append(stream, stream_payload_cls(value="first"))
    mid_id = await redis_stream.append(stream, stream_payload_cls(value="second"))
    await redis_stream.append(stream, stream_payload_cls(value="third"))

    # Read from after mid_id
    messages = await redis_stream.read({stream: mid_id}, limit=5)
    assert len(messages) == 1
    assert messages[0]["payload"].value == "third"


@pytest.mark.asyncio
async def test_stream_append_with_metadata(
    redis_stream: RedisStreamAdapter,
    stream_payload_cls,
) -> None:
    """append with type, key, timestamp encodes metadata."""
    from datetime import datetime

    stream = _stream_name()
    ts = datetime(2025, 1, 15, 12, 0, 0)
    await redis_stream.append(
        stream,
        stream_payload_cls(value="meta"),
        type="event",
        key="partition-1",
        timestamp=ts,
    )
    messages = await redis_stream.read({stream: "0"}, limit=5)
    assert len(messages) == 1
    assert messages[0]["type"] == "event"
    assert messages[0]["key"] == "partition-1"
    assert messages[0]["timestamp"] == ts


@pytest.mark.asyncio
async def test_stream_group_read(
    redis_stream: RedisStreamAdapter,
    redis_stream_group: RedisStreamGroupAdapter,
    redis_client: RedisClient,
    stream_payload_cls,
) -> None:
    """Stream group read consumes messages after group creation."""
    stream = _stream_name()
    group = "test-group"
    consumer = "c1"

    await redis_stream.append(stream, stream_payload_cls(value="one"))
    await redis_stream.append(stream, stream_payload_cls(value="two"))

    await redis_client.xgroup_create(stream, group, id="0-0", mkstream=True)

    messages = await redis_stream_group.read(group, consumer, {stream: ">"}, limit=10)
    assert len(messages) == 2
    values = [m["payload"].value for m in messages]
    assert "one" in values
    assert "two" in values


@pytest.mark.asyncio
async def test_stream_group_ack(
    redis_stream: RedisStreamAdapter,
    redis_stream_group: RedisStreamGroupAdapter,
    redis_client: RedisClient,
    stream_payload_cls,
) -> None:
    """ack runs without error and returns an int (adapter uses noack=True)."""
    stream = _stream_name()
    group = "ack-group"
    consumer = "c1"

    await redis_stream.append(stream, stream_payload_cls(value="ack-me"))
    await redis_client.xgroup_create(stream, group, id="0-0", mkstream=True)

    messages = await redis_stream_group.read(group, consumer, {stream: ">"}, limit=5)
    assert len(messages) == 1
    msg_id = messages[0]["id"]

    acked = await redis_stream_group.ack(group, stream, [msg_id])
    assert isinstance(acked, int)
    assert acked >= 0
