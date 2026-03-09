"""Integration tests for RabbitMQClient."""

from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest

from forze_rabbitmq.kernel.platform import RabbitMQClient


async def _receive_until(
    client: RabbitMQClient,
    queue: str,
    *,
    attempts: int = 8,
) -> list[dict]:
    for _ in range(attempts):
        messages = await client.receive(queue, limit=1, timeout=timedelta(seconds=1))

        if messages:
            return messages

    raise AssertionError("RabbitMQ message was not received in time")


async def _receive_exact(
    client: RabbitMQClient,
    queue: str,
    expected: int,
    *,
    attempts: int = 8,
) -> list[dict]:
    out: list[dict] = []

    for _ in range(attempts):
        remaining = expected - len(out)

        if remaining <= 0:
            return out

        messages = await client.receive(
            queue,
            limit=remaining,
            timeout=timedelta(seconds=1),
        )
        out.extend(messages)

    if len(out) == expected:
        return out

    raise AssertionError("RabbitMQ batch messages were not received in time")


@pytest.mark.asyncio
async def test_client_enqueue_receive_ack(rabbitmq_client: RabbitMQClient) -> None:
    queue = f"it:rabbitmq-client:{uuid4().hex[:12]}"
    ts = datetime(2025, 1, 15, 12, 0, 0, tzinfo=timezone.utc)

    message_id = await rabbitmq_client.enqueue(
        queue,
        b'{"value":"hello"}',
        type="created",
        key="partition-1",
        enqueued_at=ts,
    )

    messages = await _receive_until(rabbitmq_client, queue)
    message = messages[0]

    assert message["id"] == message_id
    assert message["body"] == b'{"value":"hello"}'
    assert message["type"] == "created"
    assert message["key"] == "partition-1"
    assert message["enqueued_at"] == ts

    assert await rabbitmq_client.ack(queue, [message["id"]]) == 1


@pytest.mark.asyncio
async def test_client_enqueue_many_receive_ack(rabbitmq_client: RabbitMQClient) -> None:
    queue = f"it:rabbitmq-client:{uuid4().hex[:12]}"
    ts = datetime(2025, 2, 10, 12, 0, 0, tzinfo=timezone.utc)

    message_ids = await rabbitmq_client.enqueue_many(
        queue,
        [
            b'{"value":"batch-1"}',
            b'{"value":"batch-2"}',
            b'{"value":"batch-3"}',
        ],
        type="created",
        key="partition-batch",
        enqueued_at=ts,
    )

    messages = await _receive_exact(rabbitmq_client, queue, expected=3)
    received_ids = [message["id"] for message in messages]

    assert len(message_ids) == 3
    assert set(received_ids) == set(message_ids)
    assert all(message["type"] == "created" for message in messages)
    assert all(message["key"] == "partition-batch" for message in messages)
    assert all(message["enqueued_at"] == ts for message in messages)
    assert await rabbitmq_client.ack(queue, received_ids) == 3


@pytest.mark.asyncio
async def test_client_nack_requeue_then_ack(rabbitmq_client: RabbitMQClient) -> None:
    queue = f"it:rabbitmq-client:{uuid4().hex[:12]}"
    await rabbitmq_client.enqueue(queue, b'{"value":"requeue"}')

    first = (await _receive_until(rabbitmq_client, queue))[0]

    assert await rabbitmq_client.nack(queue, [first["id"]], requeue=True) == 1

    second = (await _receive_until(rabbitmq_client, queue))[0]
    assert second["body"] == b'{"value":"requeue"}'
    assert await rabbitmq_client.ack(queue, [second["id"]]) == 1
