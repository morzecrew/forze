"""Integration tests for RedisPubSubAdapter."""

import asyncio
from contextlib import suppress
from datetime import datetime
from uuid import uuid4

import pytest

from forze_redis.adapters import RedisPubSubAdapter


def _topic_name() -> str:
    return f"it:pubsub:{uuid4().hex[:12]}"


async def _publish_until_received(
    redis_pubsub: RedisPubSubAdapter,
    topic: str,
    payload,
    **kwargs,
):
    stream = redis_pubsub.subscribe([topic])
    recv_task = asyncio.create_task(anext(stream))

    try:
        for _ in range(5):
            await redis_pubsub.publish(topic, payload, **kwargs)

            try:
                return await asyncio.wait_for(asyncio.shield(recv_task), timeout=0.5)
            except asyncio.TimeoutError:
                continue

        raise AssertionError("PubSub message was not received in time")

    finally:
        if not recv_task.done():
            recv_task.cancel()
            with suppress(asyncio.CancelledError):
                await recv_task

        await stream.aclose()


@pytest.mark.asyncio
async def test_pubsub_publish_and_subscribe(
    redis_pubsub: RedisPubSubAdapter,
    pubsub_payload_cls,
) -> None:
    topic = _topic_name()
    message = await _publish_until_received(
        redis_pubsub,
        topic,
        pubsub_payload_cls(value="hello"),
    )

    assert message["topic"] == topic
    assert message["payload"].value == "hello"


@pytest.mark.asyncio
async def test_pubsub_publish_with_metadata(
    redis_pubsub: RedisPubSubAdapter,
    pubsub_payload_cls,
) -> None:
    topic = _topic_name()
    ts = datetime(2025, 1, 15, 12, 0, 0)
    message = await _publish_until_received(
        redis_pubsub,
        topic,
        pubsub_payload_cls(value="meta"),
        type="event",
        key="partition-1",
        published_at=ts,
    )

    assert message["type"] == "event"
    assert message["key"] == "partition-1"
    assert message["published_at"] == ts
