"""Performance tests for RabbitMQQueueAdapter."""

from datetime import timedelta
from uuid import uuid4

import pytest
import pytest_asyncio
from pydantic import BaseModel

pytest.importorskip("aio_pika")

from forze_rabbitmq.adapters import RabbitMQQueueAdapter, RabbitMQQueueCodec
from forze_rabbitmq.kernel.platform import RabbitMQClient


class _QueuePayload(BaseModel):
    """Minimal payload model for queue performance tests."""

    value: str


def _perf_namespace(prefix: str) -> str:
    return f"perf:{prefix}:{uuid4().hex[:12]}"


@pytest_asyncio.fixture
async def rabbitmq_queue(
    rabbitmq_client: RabbitMQClient,
) -> RabbitMQQueueAdapter[_QueuePayload]:
    """Provide a RabbitMQQueueAdapter with a unique namespace per test."""
    return RabbitMQQueueAdapter(
        client=rabbitmq_client,
        codec=RabbitMQQueueCodec(model=_QueuePayload),
        namespace=_perf_namespace("queue"),
    )


@pytest.mark.perf
@pytest.mark.asyncio
async def test_rabbitmq_adapter_enqueue_benchmark(
    async_benchmark, rabbitmq_queue: RabbitMQQueueAdapter[_QueuePayload]
) -> None:
    """Benchmark adapter enqueue with Pydantic payload."""

    async def run() -> None:
        queue = f"jobs-{uuid4().hex[:8]}"
        await rabbitmq_queue.enqueue(queue, _QueuePayload(value="bench"))

    await async_benchmark(run)


@pytest.mark.perf
@pytest.mark.asyncio
async def test_rabbitmq_adapter_enqueue_batch_benchmark(
    async_benchmark, rabbitmq_queue: RabbitMQQueueAdapter[_QueuePayload]
) -> None:
    """Benchmark adapter batch enqueue of 10 messages."""
    queue = f"jobs-{uuid4().hex[:8]}"

    async def run() -> None:
        await rabbitmq_queue.enqueue_many(
            queue,
            [_QueuePayload(value=f"bench-{i}") for i in range(10)],
        )

    await async_benchmark(run)


@pytest.mark.perf
@pytest.mark.asyncio
async def test_rabbitmq_adapter_enqueue_receive_ack_benchmark(
    async_benchmark, rabbitmq_queue: RabbitMQQueueAdapter[_QueuePayload]
) -> None:
    """Benchmark adapter full round-trip: enqueue, receive, ack."""

    async def run() -> None:
        queue = f"jobs-{uuid4().hex[:8]}"
        msg_id = await rabbitmq_queue.enqueue(queue, _QueuePayload(value="roundtrip"))
        messages = await rabbitmq_queue.receive(
            queue, limit=1, timeout=timedelta(seconds=2)
        )
        assert len(messages) == 1
        assert messages[0]["id"] == msg_id
        await rabbitmq_queue.ack(queue, [msg_id])

    await async_benchmark(run)
