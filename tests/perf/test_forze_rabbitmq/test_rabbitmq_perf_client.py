"""Performance tests for RabbitMQClient."""

from datetime import timedelta
from uuid import uuid4

import pytest

pytest.importorskip("aio_pika")

from forze_rabbitmq.kernel.platform import RabbitMQClient


# Note: receive-only benchmarks are omitted because a pre-seeded queue is
# exhausted after the first benchmark iterations; use enqueue_receive_ack
# for round-trip receive performance.


def _perf_queue(prefix: str) -> str:
    return f"perf:{prefix}:{uuid4().hex[:12]}"


@pytest.mark.perf
@pytest.mark.asyncio
async def test_rabbitmq_enqueue_benchmark(
    async_benchmark, rabbitmq_client: RabbitMQClient
) -> None:
    """Benchmark single enqueue."""

    async def run() -> None:
        queue = _perf_queue("enq")
        await rabbitmq_client.enqueue(queue, b'{"value":"bench"}')

    await async_benchmark(run)


@pytest.mark.perf
@pytest.mark.asyncio
async def test_rabbitmq_enqueue_batch_benchmark(
    async_benchmark, rabbitmq_client: RabbitMQClient
) -> None:
    """Benchmark batch enqueue of 10 messages to the same queue."""
    queue = _perf_queue("enq_batch")

    async def run() -> None:
        await rabbitmq_client.enqueue_many(
            queue,
            [f'{{"value":"bench-{i}"}}'.encode() for i in range(10)],
        )

    await async_benchmark(run)


@pytest.mark.perf
@pytest.mark.asyncio
async def test_rabbitmq_enqueue_receive_ack_benchmark(
    async_benchmark, rabbitmq_client: RabbitMQClient
) -> None:
    """Benchmark full round-trip: enqueue, receive, ack."""
    queue = _perf_queue("roundtrip")

    async def run() -> None:
        msg_id = await rabbitmq_client.enqueue(queue, b'{"value":"rt"}')
        messages = await rabbitmq_client.receive(
            queue, limit=1, timeout=timedelta(seconds=2)
        )
        assert len(messages) == 1
        assert messages[0]["id"] == msg_id
        await rabbitmq_client.ack(queue, [msg_id])

    await async_benchmark(run)


@pytest.mark.perf
@pytest.mark.asyncio
async def test_rabbitmq_channel_benchmark(
    async_benchmark, rabbitmq_client: RabbitMQClient
) -> None:
    """Benchmark channel context (open/close)."""

    async def run() -> None:
        async with rabbitmq_client.channel():
            pass

    await async_benchmark(run)
