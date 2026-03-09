"""Integration tests for SQSClient."""

from datetime import datetime, timedelta, timezone

import pytest

from forze_sqs.kernel.platform import SQSClient


async def _receive_until(
    client: SQSClient,
    queue: str,
    *,
    attempts: int = 8,
) -> list[dict]:
    for _ in range(attempts):
        messages = await client.receive(queue, limit=1, timeout=timedelta(seconds=1))

        if messages:
            return messages

    raise AssertionError("SQS message was not received in time")


@pytest.mark.asyncio
async def test_client_enqueue_receive_ack(sqs_client: SQSClient, sqs_queue_url: str) -> None:
    ts = datetime(2025, 1, 15, 12, 0, 0, tzinfo=timezone.utc)

    async with sqs_client.client():
        message_id = await sqs_client.enqueue(
            sqs_queue_url,
            b'{"value":"hello"}',
            type="created",
            key="partition-1",
            enqueued_at=ts,
        )

        messages = await _receive_until(sqs_client, sqs_queue_url)
        message = messages[0]

        assert message["queue"] == sqs_queue_url
        assert message["id"]
        assert message["body"] == b'{"value":"hello"}'
        assert message["type"] == "created"
        assert message["key"] == "partition-1"
        assert message["enqueued_at"] == ts
        assert message_id

        assert await sqs_client.ack(sqs_queue_url, [message["id"]]) == 1
        assert await sqs_client.receive(sqs_queue_url, limit=1) == []


@pytest.mark.asyncio
async def test_client_nack_requeue_then_ack(
    sqs_client: SQSClient,
    sqs_queue_url: str,
) -> None:
    async with sqs_client.client():
        await sqs_client.enqueue(sqs_queue_url, b'{"value":"requeue"}')

        first = (await _receive_until(sqs_client, sqs_queue_url))[0]

        assert await sqs_client.nack(sqs_queue_url, [first["id"]], requeue=True) == 1

        second = (await _receive_until(sqs_client, sqs_queue_url))[0]
        assert second["body"] == b'{"value":"requeue"}'
        assert await sqs_client.ack(sqs_queue_url, [second["id"]]) == 1
