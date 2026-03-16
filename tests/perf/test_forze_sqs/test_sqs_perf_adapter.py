"""Performance tests for SQSQueueAdapter."""

from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest

pytest.importorskip("aioboto3")

from forze_sqs.adapters import SQSQueueAdapter
from forze_sqs.kernel.platform import SQSClient


async def _ensure_queue(
    sqs_client: SQSClient,
    sqs_queue: SQSQueueAdapter,
    queue: str,
) -> None:
    async with sqs_client.client():
        physical_queue = (
            f"{sqs_queue.namespace}-{queue}" if sqs_queue.namespace else queue
        )
        await sqs_client.create_queue(physical_queue)


@pytest.mark.perf
@pytest.mark.asyncio
async def test_adapter_enqueue_benchmark(
    async_benchmark,
    sqs_client: SQSClient,
    sqs_queue: SQSQueueAdapter,
    queue_payload_cls,
) -> None:
    """Benchmark adapter enqueue with Pydantic payload."""

    async def run() -> None:
        queue = f"jobs-{uuid4().hex[:8]}"
        await _ensure_queue(sqs_client, sqs_queue, queue)
        await sqs_queue.enqueue(queue, queue_payload_cls(value="bench"))

    await async_benchmark(run)


@pytest.mark.perf
@pytest.mark.asyncio
async def test_adapter_enqueue_batch_benchmark(
    async_benchmark,
    sqs_client: SQSClient,
    sqs_queue: SQSQueueAdapter,
    queue_payload_cls,
) -> None:
    """Benchmark adapter batch enqueue of 10 messages."""
    queue = f"jobs-{uuid4().hex[:8]}"
    ts = datetime(2025, 2, 1, 12, 0, 0, tzinfo=timezone.utc)
    await _ensure_queue(sqs_client, sqs_queue, queue)

    async def run() -> None:
        await sqs_queue.enqueue_many(
            queue,
            [queue_payload_cls(value=f"bench-{i}") for i in range(10)],
            type="created",
            key="partition",
            enqueued_at=ts,
        )

    await async_benchmark(run)


@pytest.mark.perf
@pytest.mark.asyncio
async def test_adapter_enqueue_receive_ack_benchmark(
    async_benchmark,
    sqs_client: SQSClient,
    sqs_queue: SQSQueueAdapter,
    queue_payload_cls,
) -> None:
    """Benchmark adapter full round-trip: enqueue, receive, ack."""

    async def run() -> None:
        queue = f"jobs-{uuid4().hex[:8]}"
        await _ensure_queue(sqs_client, sqs_queue, queue)
        await sqs_queue.enqueue(queue, queue_payload_cls(value="roundtrip"))
        messages = await sqs_queue.receive(queue, limit=1, timeout=timedelta(seconds=2))
        assert len(messages) == 1
        assert messages[0]["id"]
        await sqs_queue.ack(queue, [messages[0]["id"]])

    await async_benchmark(run)
