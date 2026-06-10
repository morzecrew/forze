"""Integration tests for RabbitMQClient receive/consume timeout semantics."""

import asyncio
import time
from contextlib import aclosing
from datetime import timedelta
from uuid import uuid4

import pytest

from forze_rabbitmq.kernel.client import RabbitMQClient


@pytest.mark.asyncio
async def test_consume_none_timeout_survives_idle_gap(
    rabbitmq_client: RabbitMQClient,
) -> None:
    """consume(timeout=None) stays alive across a >1.5s idle gap."""
    queue = f"it:rabbitmq-consume:{uuid4().hex[:12]}"
    received: list = []

    async def _consume_one() -> None:
        async with aclosing(rabbitmq_client.consume(queue)) as stream:
            async for message in stream:
                received.append(message)
                break

    task = asyncio.create_task(_consume_one())

    # Idle gap longer than the old 1s default that used to crash consumers.
    await asyncio.sleep(1.6)
    assert not task.done(), "consumer terminated during the idle gap"

    await rabbitmq_client.enqueue(queue, b'{"value":"after-gap"}')
    await asyncio.wait_for(task, timeout=3)

    assert len(received) == 1
    assert received[0].body == b'{"value":"after-gap"}'
    assert await rabbitmq_client.ack(queue, [received[0].id]) == 1


@pytest.mark.asyncio
async def test_consume_finite_timeout_stops_cleanly_when_idle(
    rabbitmq_client: RabbitMQClient,
) -> None:
    """A finite idle timeout ends the stream without raising."""
    queue = f"it:rabbitmq-consume:{uuid4().hex[:12]}"

    async def _drain() -> list:
        return [
            message
            async for message in rabbitmq_client.consume(
                queue, timeout=timedelta(seconds=1)
            )
        ]

    received = await asyncio.wait_for(_drain(), timeout=4)

    assert received == []


@pytest.mark.asyncio
async def test_receive_none_timeout_returns_partial_batch_bounded(
    rabbitmq_client: RabbitMQClient,
) -> None:
    """receive(limit=5, timeout=None) returns the 1 available message quickly."""
    queue = f"it:rabbitmq-consume:{uuid4().hex[:12]}"
    await rabbitmq_client.enqueue(queue, b'{"value":"only-one"}')

    start = time.monotonic()
    messages = await asyncio.wait_for(
        rabbitmq_client.receive(queue, limit=5),
        timeout=5,
    )
    elapsed = time.monotonic() - start

    assert len(messages) == 1
    assert messages[0].body == b'{"value":"only-one"}'
    assert elapsed < 4.0
    assert await rabbitmq_client.ack(queue, [messages[0].id]) == 1


@pytest.mark.asyncio
async def test_receive_none_timeout_empty_queue_returns_bounded(
    rabbitmq_client: RabbitMQClient,
) -> None:
    """receive(timeout=None) on an empty queue does not hang."""
    queue = f"it:rabbitmq-consume:{uuid4().hex[:12]}"

    messages = await asyncio.wait_for(rabbitmq_client.receive(queue), timeout=5)

    assert messages == []
