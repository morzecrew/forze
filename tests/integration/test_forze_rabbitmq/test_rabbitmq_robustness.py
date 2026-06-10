"""Integration tests for RabbitMQ robustness fixes.

Covers: poison messages not wedging a consumer, per-delay-value queues
eliminating head-of-line blocking, and ``close()`` returning pending
deliveries to the broker promptly.
"""

import asyncio
import time
from datetime import timedelta
from urllib.parse import quote
from uuid import uuid4

import pytest
from testcontainers.rabbitmq import RabbitMqContainer

pytest.importorskip("aio_pika")

from forze.base.serialization import PydanticModelCodec
from forze_rabbitmq.adapters import RabbitMQQueueAdapter, RabbitMQQueueCodec
from forze_rabbitmq.kernel.client import RabbitMQClient, RabbitMQConfig

# ----------------------- #


def _dsn(container: RabbitMqContainer) -> str:
    host = container.get_container_host_ip()
    port = container.get_exposed_port(container.port)
    vhost = quote(container.vhost, safe="")

    return f"amqp://{container.username}:{container.password}@{host}:{port}/{vhost}"


async def _receive_until(
    adapter: RabbitMQQueueAdapter,
    queue: str,
    *,
    attempts: int = 8,
    window: timedelta = timedelta(seconds=1),
):
    for _ in range(attempts):
        messages = await adapter.receive(queue, limit=1, timeout=window)

        if messages:
            return messages[0]

    raise AssertionError("Queue message was not received in time")


# ----------------------- #


@pytest.mark.asyncio
async def test_poison_message_is_skipped_and_removed(
    rabbitmq_queue: RabbitMQQueueAdapter,
    queue_payload_cls,
) -> None:
    """A garbage payload is nacked away; the consumer still yields the good one."""
    queue = f"jobs-poison-{uuid4().hex[:8]}"
    physical_queue = f"{rabbitmq_queue.namespace}:{queue}"

    # Poison first so the consumer hits it before the good message.
    await rabbitmq_queue.client.enqueue(physical_queue, b"\x00not-json")
    await rabbitmq_queue.enqueue(queue, queue_payload_cls(value="survivor"))

    stream = rabbitmq_queue.consume(queue, timeout=timedelta(seconds=2))
    message = await asyncio.wait_for(anext(stream), timeout=10)
    await stream.aclose()

    assert message.payload.value == "survivor"
    assert await rabbitmq_queue.ack(queue, [message.id]) == 1

    # The poison message was removed from the queue (requeue=False without a
    # DLX configured on the work queue drops it) — it must not come back.
    leftover = await rabbitmq_queue.receive(
        queue,
        limit=1,
        timeout=timedelta(milliseconds=400),
    )
    assert leftover == []


@pytest.mark.asyncio
async def test_poison_message_in_receive_batch(
    rabbitmq_queue: RabbitMQQueueAdapter,
    queue_payload_cls,
) -> None:
    """Batch receive returns the decodable remainder and drops the poison entry."""
    queue = f"jobs-poison-batch-{uuid4().hex[:8]}"
    physical_queue = f"{rabbitmq_queue.namespace}:{queue}"

    await rabbitmq_queue.client.enqueue(physical_queue, b"\x00not-json")
    await rabbitmq_queue.enqueue(queue, queue_payload_cls(value="survivor"))

    good = []

    for _ in range(8):
        good.extend(await rabbitmq_queue.receive(queue, limit=2, timeout=timedelta(seconds=1)))

        if good:
            break

    assert [m.payload.value for m in good] == ["survivor"]
    assert await rabbitmq_queue.ack(queue, [good[0].id]) == 1


# ....................... #


@pytest.mark.asyncio
async def test_delayed_delivery_no_head_of_line_blocking(
    rabbitmq_delayed_queue: RabbitMQQueueAdapter,
    queue_payload_cls,
) -> None:
    """A short delay enqueued *after* a long one must not wait for it.

    With a single shared delay queue the 1s message would sit behind the 4s
    one (RabbitMQ only expires from the queue head); per-delay-value queues
    deliver it in ~1s.
    """
    queue = f"jobs-hol-{uuid4().hex[:8]}"

    await rabbitmq_delayed_queue.enqueue(
        queue,
        queue_payload_cls(value="slow"),
        delay=timedelta(seconds=4),
    )
    await rabbitmq_delayed_queue.enqueue(
        queue,
        queue_payload_cls(value="fast"),
        delay=timedelta(seconds=1),
    )

    start = time.monotonic()

    first = await _receive_until(
        rabbitmq_delayed_queue,
        queue,
        attempts=10,
        window=timedelta(milliseconds=500),
    )
    first_elapsed = time.monotonic() - start

    assert first.payload.value == "fast"
    assert first_elapsed < 3.0, (
        f"1s-delayed message arrived after {first_elapsed:.2f}s — "
        "head-of-line blocked behind the 4s delay"
    )
    assert await rabbitmq_delayed_queue.ack(queue, [first.id]) == 1

    second = await _receive_until(
        rabbitmq_delayed_queue,
        queue,
        attempts=12,
        window=timedelta(milliseconds=500),
    )
    total_elapsed = time.monotonic() - start

    assert second.payload.value == "slow"
    assert total_elapsed < 7.5
    assert await rabbitmq_delayed_queue.ack(queue, [second.id]) == 1


# ....................... #


@pytest.mark.asyncio
async def test_close_with_pending_redelivers_promptly(
    rabbitmq_container: RabbitMqContainer,
    queue_payload_cls,
) -> None:
    """close() nacks unacked deliveries so a fresh consumer gets them quickly."""
    dsn = _dsn(rabbitmq_container)
    namespace = f"it:rabbitmq-close:{uuid4().hex[:12]}"
    queue = f"jobs-close-{uuid4().hex[:8]}"
    codec = RabbitMQQueueCodec(payload_codec=PydanticModelCodec(queue_payload_cls))
    config = RabbitMQConfig(prefetch_count=20, connect_timeout=timedelta(seconds=10))

    first_client = RabbitMQClient()
    await first_client.initialize(dsn=dsn, config=config)
    first_adapter = RabbitMQQueueAdapter(
        client=first_client,
        codec=codec,
        namespace=namespace,
    )

    await first_adapter.enqueue(queue, queue_payload_cls(value="orphaned"))

    received = await first_adapter.receive(queue, limit=1, timeout=timedelta(seconds=2))
    assert len(received) == 1  # now pending/unacked on first_client

    await first_client.close()

    second_client = RabbitMQClient()
    await second_client.initialize(dsn=dsn, config=config)

    try:
        second_adapter = RabbitMQQueueAdapter(
            client=second_client,
            codec=codec,
            namespace=namespace,
        )

        redelivered = await _receive_until(second_adapter, queue, attempts=5)

        assert redelivered.payload.value == "orphaned"
        assert await second_adapter.ack(queue, [redelivered.id]) == 1
    finally:
        await second_client.close()
