"""Envelope headers ride the Redis JSON envelopes (stream + pubsub)."""

from __future__ import annotations

import asyncio
from datetime import timedelta
from uuid import uuid4

import pytest

pytest.importorskip("redis")

from forze.application.contracts.envelope import (
    HEADER_CORRELATION_ID,
    HEADER_EVENT_ID,
)
from forze_redis.adapters import RedisPubSubAdapter, RedisStreamAdapter
from forze_redis.kernel.client import RedisClient

# ----------------------- #


def _stream_name() -> str:
    return f"it:envelope:{uuid4().hex[:12]}"


# ----------------------- #


@pytest.mark.asyncio
async def test_stream_headers_round_trip(
    redis_stream: RedisStreamAdapter,
    stream_payload_cls,
) -> None:
    stream = _stream_name()
    headers = {
        HEADER_CORRELATION_ID: "corr-1",
        HEADER_EVENT_ID: "evt-1",
        "trace": "t-1",
    }

    msg_id = await redis_stream.append(
        stream,
        stream_payload_cls(value="hello"),
        type="created",
        key="k-1",
        headers=headers,
    )

    [message] = await redis_stream.read({stream: "0"}, limit=10)

    assert message.id == msg_id
    assert dict(message.headers) == headers
    assert message.type == "created"
    assert message.key == "k-1"


@pytest.mark.asyncio
async def test_stream_without_headers_decodes_empty_mapping(
    redis_stream: RedisStreamAdapter,
    stream_payload_cls,
) -> None:
    stream = _stream_name()
    await redis_stream.append(stream, stream_payload_cls(value="hello"))

    [message] = await redis_stream.read({stream: "0"}, limit=10)

    assert dict(message.headers) == {}


@pytest.mark.asyncio
async def test_stream_group_read_decodes_headers_too(
    redis_stream: RedisStreamAdapter,
    redis_stream_group,
    redis_client: RedisClient,
    stream_payload_cls,
) -> None:
    stream = _stream_name()
    group = f"grp-{uuid4().hex[:8]}"

    await redis_stream.append(
        stream,
        stream_payload_cls(value="hello"),
        headers={HEADER_CORRELATION_ID: "corr-2"},
    )
    await redis_client.xgroup_create(stream, group, id="0-0", mkstream=True)

    [message] = await redis_stream_group.read(
        group,
        "consumer-1",
        {stream: ">"},
        limit=10,
    )

    assert message.headers[HEADER_CORRELATION_ID] == "corr-2"

    acked = await redis_stream_group.ack(group, stream, [message.id])
    assert acked == 1


@pytest.mark.asyncio
async def test_pubsub_headers_round_trip(
    redis_pubsub: RedisPubSubAdapter,
    pubsub_payload_cls,
) -> None:
    topic = f"it:envelope:{uuid4().hex[:12]}"
    headers = {HEADER_CORRELATION_ID: "corr-3", "trace": "t-1"}
    received = []

    async def _subscriber() -> None:
        async for message in redis_pubsub.subscribe(
            [topic], timeout=timedelta(seconds=5)
        ):
            received.append(message)
            return

    task = asyncio.create_task(_subscriber())
    await asyncio.sleep(0.3)  # let the subscription register

    await redis_pubsub.publish(
        topic,
        pubsub_payload_cls(value="hello"),
        type="created",
        headers=headers,
    )

    await asyncio.wait_for(task, timeout=10)

    assert len(received) == 1
    assert dict(received[0].headers) == headers
    assert received[0].type == "created"
