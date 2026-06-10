"""Integration tests for SQSClient.consume long-polling / idle-timeout semantics."""

import asyncio
from contextlib import aclosing
from datetime import timedelta

import pytest

from forze_sqs.kernel.client import SQSClient


@pytest.mark.asyncio
async def test_consume_none_timeout_survives_idle_gap(
    sqs_client: SQSClient,
    sqs_queue_url: str,
) -> None:
    """consume(timeout=None) long-polls through an idle gap and picks up a late message."""
    received: list = []

    async def _consume_one() -> None:
        async with sqs_client.client():
            async with aclosing(sqs_client.consume(sqs_queue_url)) as stream:
                async for message in stream:
                    received.append(message)
                    break

    task = asyncio.create_task(_consume_one())

    await asyncio.sleep(1.6)
    assert not task.done(), "consumer terminated during the idle gap"

    async with sqs_client.client():
        await sqs_client.enqueue(sqs_queue_url, b'{"value":"after-gap"}')

    await asyncio.wait_for(task, timeout=5)

    assert len(received) == 1
    assert received[0].body == b'{"value":"after-gap"}'

    async with sqs_client.client():
        assert await sqs_client.ack(sqs_queue_url, [received[0].id]) == 1


@pytest.mark.asyncio
async def test_consume_finite_timeout_stops_cleanly_when_idle(
    sqs_client: SQSClient,
    sqs_queue_url: str,
) -> None:
    """A finite idle timeout ends the stream without raising."""

    async def _drain() -> list:
        async with sqs_client.client():
            return [
                message
                async for message in sqs_client.consume(
                    sqs_queue_url, timeout=timedelta(seconds=1)
                )
            ]

    received = await asyncio.wait_for(_drain(), timeout=5)

    assert received == []
