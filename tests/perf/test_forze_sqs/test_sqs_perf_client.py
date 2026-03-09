"""Performance tests for SQSClient."""

from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest

pytest.importorskip("aioboto3")

from forze_sqs.kernel.platform import SQSClient


# Note: receive-only benchmarks are omitted because a pre-seeded queue is
# exhausted after the first benchmark iterations; use enqueue_receive_ack
# for round-trip receive performance.


def _perf_queue(prefix: str) -> str:
    return f"perf:{prefix}:{uuid4().hex[:12]}"


@pytest.mark.perf
@pytest.mark.asyncio
async def test_health_benchmark(
    async_benchmark, sqs_client: SQSClient
) -> None:
    """Benchmark SQS health check (list_queues)."""

    async def run() -> None:
        async with sqs_client.client():
            status, ok = await sqs_client.health()
            assert ok

    await async_benchmark(run)


@pytest.mark.perf
@pytest.mark.asyncio
async def test_client_context_benchmark(
    async_benchmark, sqs_client: SQSClient
) -> None:
    """Benchmark client context manager (open/close)."""

    async def run() -> None:
        async with sqs_client.client():
            pass

    await async_benchmark(run)


@pytest.mark.perf
@pytest.mark.asyncio
async def test_enqueue_benchmark(
    async_benchmark, sqs_client: SQSClient
) -> None:
    """Benchmark single enqueue."""

    async def run() -> None:
        queue = _perf_queue("enq")
        async with sqs_client.client():
            await sqs_client.create_queue(queue)
            await sqs_client.enqueue(queue, b'{"value":"bench"}')

    await async_benchmark(run)


@pytest.mark.perf
@pytest.mark.asyncio
async def test_enqueue_batch_benchmark(
    async_benchmark, sqs_client: SQSClient
) -> None:
    """Benchmark batch enqueue of 10 messages to the same queue."""
    queue = _perf_queue("enq_batch")
    ts = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

    async with sqs_client.client():
        await sqs_client.create_queue(queue)

    async def run() -> None:
        async with sqs_client.client():
            await sqs_client.enqueue_many(
                queue,
                [f'{{"value":"bench-{i}"}}'.encode() for i in range(10)],
                type="created",
                key="partition",
                enqueued_at=ts,
            )

    await async_benchmark(run)


@pytest.mark.perf
@pytest.mark.asyncio
async def test_enqueue_receive_ack_benchmark(
    async_benchmark, sqs_client: SQSClient
) -> None:
    """Benchmark full round-trip: enqueue, receive, ack."""
    queue = _perf_queue("roundtrip")
    ts = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

    async with sqs_client.client():
        await sqs_client.create_queue(queue)

    async def run() -> None:
        async with sqs_client.client():
            await sqs_client.enqueue(queue, b'{"value":"rt"}', enqueued_at=ts)
            messages = await sqs_client.receive(
                queue, limit=1, timeout=timedelta(seconds=2)
            )
            assert len(messages) == 1
            assert messages[0]["id"]
            await sqs_client.ack(queue, [messages[0]["id"]])

    await async_benchmark(run)
