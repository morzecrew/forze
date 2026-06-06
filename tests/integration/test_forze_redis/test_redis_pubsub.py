"""Integration tests for RedisPubSubAdapter."""

import asyncio
from contextlib import suppress
from datetime import datetime, timedelta
from uuid import uuid4

import pytest

from forze_redis.adapters import RedisPubSubAdapter
from forze_redis.kernel.client import RedisClient, RedisConfig


def _topic_name() -> str:
    return f"it:pubsub:{uuid4().hex[:12]}"


async def _publish_until_received(
    redis_pubsub: RedisPubSubAdapter,
    topic: str,
    payload,
    **kwargs,
):
    stream = redis_pubsub.subscribe([topic], timeout=timedelta(seconds=2))
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

    assert message.topic == topic
    assert message.payload.value == "hello"


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

    assert message.type == "event"
    assert message.key == "partition-1"
    assert message.published_at == ts


def _channel_name() -> str:
    return f"it:client-pubsub:{uuid4().hex[:12]}"


async def _client_subscribe_receive(
    client: RedisClient,
    channel: str,
    payload: bytes,
    *,
    timeout=None,
):
    """Drive ``RedisClient.subscribe`` end to end: subscribe, publish, receive."""
    stream = client.subscribe([channel], timeout=timeout)
    recv_task = asyncio.create_task(anext(stream))

    try:
        # Give the subscription time to register before publishing.
        for _ in range(20):
            await asyncio.sleep(0.05)
            await client.publish(channel, payload)

            try:
                return await asyncio.wait_for(
                    asyncio.shield(recv_task), timeout=0.2
                )
            except asyncio.TimeoutError:
                continue

        raise AssertionError("client.subscribe message was not received in time")

    finally:
        if not recv_task.done():
            recv_task.cancel()
            with suppress(asyncio.CancelledError):
                await recv_task

        await stream.aclose()


@pytest.mark.asyncio
async def test_client_subscribe_receives_published_message(
    redis_client: RedisClient,
) -> None:
    """End-to-end client-level subscribe -> publish -> receive -> close."""
    channel = _channel_name()

    chan, payload = await _client_subscribe_receive(
        redis_client,
        channel,
        b"raw-payload",
        timeout=timedelta(seconds=2),
    )

    assert chan == channel
    assert payload == b"raw-payload"


@pytest.mark.asyncio
async def test_client_subscribe_no_timeout_polls(
    redis_client: RedisClient,
) -> None:
    """subscribe with timeout=None busy-polls (no-timeout branch) and still delivers."""
    channel = _channel_name()

    chan, payload = await _client_subscribe_receive(
        redis_client,
        channel,
        b"poll-payload",
        timeout=None,
    )

    assert chan == channel
    assert payload == b"poll-payload"


@pytest.mark.asyncio
async def test_client_subscribe_empty_channels_is_noop(
    redis_client: RedisClient,
) -> None:
    """subscribe with no channels yields nothing and closes cleanly."""
    stream = redis_client.subscribe([])
    with pytest.raises(StopAsyncIteration):
        await anext(stream)
    await stream.aclose()


@pytest.mark.asyncio
async def test_client_subscribe_auto_reconnect_path(
    redis_container,
) -> None:
    """With pubsub_auto_reconnect=True, the reconnect-capable loop delivers messages."""
    host = redis_container.get_container_host_ip()
    port = redis_container.get_exposed_port(6379)
    dsn = f"redis://{host}:{port}/0"

    client = RedisClient()
    await client.initialize(
        dsn=dsn,
        config=RedisConfig(max_size=5, pubsub_auto_reconnect=True),
    )

    try:
        channel = _channel_name()
        chan, payload = await _client_subscribe_receive(
            client,
            channel,
            b"reconnect-payload",
            timeout=timedelta(seconds=2),
        )

        assert chan == channel
        assert payload == b"reconnect-payload"

        # Exercise the no-timeout busy-poll branch on the reconnect-capable loop.
        chan, payload = await _client_subscribe_receive(
            client,
            _channel_name(),
            b"reconnect-poll",
            timeout=None,
        )
        assert payload == b"reconnect-poll"
    finally:
        await client.close()
