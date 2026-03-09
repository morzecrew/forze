"""Integration tests for SQSQueueAdapter."""

import asyncio
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest

from forze_sqs.adapters import SQSQueueAdapter
from forze_sqs.kernel.platform import SQSClient


async def _receive_until(
    adapter: SQSQueueAdapter,
    queue: str,
    *,
    attempts: int = 8,
):
    for _ in range(attempts):
        messages = await adapter.receive(queue, limit=1, timeout=timedelta(seconds=1))

        if messages:
            return messages[0]

    raise AssertionError("Queue message was not received in time")


async def _receive_exact(
    adapter: SQSQueueAdapter,
    queue: str,
    expected: int,
    *,
    attempts: int = 8,
):
    out = []

    for _ in range(attempts):
        remaining = expected - len(out)

        if remaining <= 0:
            return out

        messages = await adapter.receive(
            queue,
            limit=remaining,
            timeout=timedelta(seconds=1),
        )
        out.extend(messages)

    if len(out) == expected:
        return out

    raise AssertionError("Queue batch message was not received in time")


async def _ensure_queue(
    sqs_client: SQSClient,
    sqs_queue: SQSQueueAdapter,
    queue: str,
) -> None:
    physical_queue = (
        f"{sqs_queue.namespace}-{queue}" if sqs_queue.namespace else queue
    )
    async with sqs_client.client():
        await sqs_client.create_queue(physical_queue)


@pytest.mark.asyncio
async def test_queue_adapter_enqueue_receive_ack(
    sqs_client: SQSClient,
    sqs_queue: SQSQueueAdapter,
    queue_payload_cls,
) -> None:
    queue = f"jobs-{uuid4().hex[:8]}"
    await _ensure_queue(sqs_client, sqs_queue, queue)
    ts = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

    message_id = await sqs_queue.enqueue(
        queue,
        queue_payload_cls(value="hello"),
        type="created",
        key="partition-a",
        enqueued_at=ts,
    )

    message = await _receive_until(sqs_queue, queue)

    assert message["id"]
    assert message["queue"] == queue
    assert message["payload"].value == "hello"
    assert message["type"] == "created"
    assert message["key"] == "partition-a"
    assert message["enqueued_at"] == ts
    assert message_id

    assert await sqs_queue.ack(queue, [message["id"]]) == 1


@pytest.mark.asyncio
async def test_queue_adapter_enqueue_many_receive_ack(
    sqs_client: SQSClient,
    sqs_queue: SQSQueueAdapter,
    queue_payload_cls,
) -> None:
    queue = f"jobs-{uuid4().hex[:8]}"
    await _ensure_queue(sqs_client, sqs_queue, queue)
    ts = datetime(2025, 2, 1, 12, 0, 0, tzinfo=timezone.utc)

    message_ids = await sqs_queue.enqueue_many(
        queue,
        [
            queue_payload_cls(value="hello-1"),
            queue_payload_cls(value="hello-2"),
            queue_payload_cls(value="hello-3"),
        ],
        type="created",
        key="partition-b",
        enqueued_at=ts,
    )

    messages = await _receive_exact(sqs_queue, queue, expected=3)
    received_ids = [message["id"] for message in messages]

    assert len(message_ids) == 3
    assert len(received_ids) == 3
    assert all(message["queue"] == queue for message in messages)
    assert {message["payload"].value for message in messages} == {
        "hello-1",
        "hello-2",
        "hello-3",
    }
    assert all(message["type"] == "created" for message in messages)
    assert all(message["key"] == "partition-b" for message in messages)
    assert all(message["enqueued_at"] == ts for message in messages)
    assert await sqs_queue.ack(queue, received_ids) == 3


@pytest.mark.asyncio
async def test_queue_adapter_consume(
    sqs_client: SQSClient,
    sqs_queue: SQSQueueAdapter,
    queue_payload_cls,
) -> None:
    queue = f"jobs-{uuid4().hex[:8]}"
    await _ensure_queue(sqs_client, sqs_queue, queue)
    stream = sqs_queue.consume(queue, timeout=timedelta(seconds=1))

    await sqs_queue.enqueue(queue, queue_payload_cls(value="consume"))
    message = await asyncio.wait_for(anext(stream), timeout=5)
    await stream.aclose()

    assert message["queue"] == queue
    assert message["payload"].value == "consume"
    assert await sqs_queue.ack(queue, [message["id"]]) == 1
