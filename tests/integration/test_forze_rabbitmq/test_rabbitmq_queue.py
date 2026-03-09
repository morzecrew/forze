"""Integration tests for RabbitMQQueueAdapter."""

import asyncio
from datetime import datetime, timedelta
from uuid import uuid4

import pytest

from forze_rabbitmq.adapters import RabbitMQQueueAdapter


async def _receive_until(
    adapter: RabbitMQQueueAdapter,
    queue: str,
    *,
    attempts: int = 8,
):
    for _ in range(attempts):
        messages = await adapter.receive(queue, limit=1, timeout=timedelta(seconds=1))

        if messages:
            return messages[0]

    raise AssertionError("Queue message was not received in time")


@pytest.mark.asyncio
async def test_queue_adapter_enqueue_receive_ack(
    rabbitmq_queue: RabbitMQQueueAdapter,
    queue_payload_cls,
) -> None:
    queue = f"jobs-{uuid4().hex[:8]}"
    ts = datetime(2025, 1, 1, 12, 0, 0)

    message_id = await rabbitmq_queue.enqueue(
        queue,
        queue_payload_cls(value="hello"),
        type="created",
        key="partition-a",
        enqueued_at=ts,
    )

    message = await _receive_until(rabbitmq_queue, queue)

    assert message["id"] == message_id
    assert message["queue"] == queue
    assert message["payload"].value == "hello"
    assert message["type"] == "created"
    assert message["key"] == "partition-a"
    assert message["enqueued_at"] == ts

    assert await rabbitmq_queue.ack(queue, [message["id"]]) == 1


@pytest.mark.asyncio
async def test_queue_adapter_consume(
    rabbitmq_queue: RabbitMQQueueAdapter,
    queue_payload_cls,
) -> None:
    queue = f"jobs-{uuid4().hex[:8]}"
    stream = rabbitmq_queue.consume(queue, timeout=timedelta(seconds=1))

    await rabbitmq_queue.enqueue(queue, queue_payload_cls(value="consume"))
    message = await asyncio.wait_for(anext(stream), timeout=5)
    await stream.aclose()

    assert message["queue"] == queue
    assert message["payload"].value == "consume"
    assert await rabbitmq_queue.ack(queue, [message["id"]]) == 1
