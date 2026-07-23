"""Integration tests for SQSClient."""

import asyncio
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from forze_sqs.kernel.client import SQSClient


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
async def test_client_enqueue_receive_ack(
    sqs_client: SQSClient, sqs_queue_url: str
) -> None:
    ts = datetime(2025, 1, 15, 12, 0, 0, tzinfo=UTC)

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

        assert message.queue == sqs_queue_url
        # enqueue() returns the broker MessageId; the received id correlates
        # with it, while the per-delivery receipt handle is separate.
        assert message_id
        assert message.id == message_id
        assert message.receipt_handle
        assert message.receipt_handle != message.id
        assert message.body == b'{"value":"hello"}'
        assert message.type == "created"
        assert message.key == "partition-1"
        assert message.enqueued_at == ts

        assert await sqs_client.ack(sqs_queue_url, [message.id]) == 1
        assert await sqs_client.receive(sqs_queue_url, limit=1) == []


@pytest.mark.asyncio
async def test_client_nack_requeue_then_ack(
    sqs_client: SQSClient,
    sqs_queue_url: str,
) -> None:
    async with sqs_client.client():
        await sqs_client.enqueue(sqs_queue_url, b'{"value":"requeue"}')

        first = (await _receive_until(sqs_client, sqs_queue_url))[0]

        assert await sqs_client.nack(sqs_queue_url, [first.id], requeue=True) == 1

        second = (await _receive_until(sqs_client, sqs_queue_url))[0]
        assert second.body == b'{"value":"requeue"}'
        assert await sqs_client.ack(sqs_queue_url, [second.id]) == 1


@pytest.mark.asyncio
async def test_client_uncounted_requeue_resets_the_receive_count(
    sqs_client: SQSClient,
    sqs_queue_url: str,
) -> None:
    """``nack(requeue=True, count=False)`` on a standard queue replaces the message with
    a byte-identical copy — fresh broker ``MessageId``, receive count back to one — so an
    outage's requeue cycles can never creep toward the redrive policy's
    ``maxReceiveCount`` and dead-letter a healthy message."""

    async with sqs_client.client():
        await sqs_client.enqueue(
            sqs_queue_url, b'{"value":"uncounted"}', type="created", key="k1"
        )

        first = (await _receive_until(sqs_client, sqs_queue_url))[0]
        assert first.delivery_count == 1

        assert (
            await sqs_client.nack(sqs_queue_url, [first.id], requeue=True, count=False)
            == 1
        )

        second = (await _receive_until(sqs_client, sqs_queue_url))[0]
        # The payload and routing context survive byte-identically…
        assert second.body == b'{"value":"uncounted"}'
        assert second.type == "created"
        assert second.key == "k1"
        # …under a fresh broker identity whose receive count restarted.
        assert second.id != first.id
        assert second.delivery_count == 1

        # A counted requeue, by contrast, raises the tally on redelivery.
        assert await sqs_client.nack(sqs_queue_url, [second.id], requeue=True) == 1
        third = (await _receive_until(sqs_client, sqs_queue_url))[0]
        assert third.id == second.id
        assert third.delivery_count == 2

        assert await sqs_client.ack(sqs_queue_url, [third.id]) == 1


@pytest.mark.asyncio
async def test_client_message_id_stable_across_redeliveries(
    sqs_client: SQSClient,
) -> None:
    """The exposed id (broker MessageId) survives a visibility-timeout
    redelivery while the receipt handle changes per delivery."""

    queue = f"forze-redeliver-{uuid4().hex[:10]}"

    async with sqs_client.client():
        url = await sqs_client.create_queue(
            queue,
            attributes={"VisibilityTimeout": "1"},
        )
        message_id = await sqs_client.enqueue(url, b'{"value":"again"}')

        first = (await _receive_until(sqs_client, url))[0]
        assert first.id == message_id

        # Let the visibility timeout lapse without acking.
        await asyncio.sleep(1.5)

        second = (await _receive_until(sqs_client, url))[0]
        assert second.id == first.id
        assert second.receipt_handle != first.receipt_handle

        # Ack by the stable id settles the latest delivery.
        assert await sqs_client.ack(url, [second.id]) == 1
        assert await sqs_client.receive(url, limit=1) == []


@pytest.mark.asyncio
async def test_client_nack_without_requeue_leaves_message_for_redrive(
    sqs_client: SQSClient,
) -> None:
    """nack(requeue=False) must not delete: the message stays invisible until
    its visibility timeout lapses and then reappears (counting toward the
    queue's redrive policy) instead of being permanently lost."""

    queue = f"forze-redrive-{uuid4().hex[:10]}"

    async with sqs_client.client():
        url = await sqs_client.create_queue(
            queue,
            attributes={"VisibilityTimeout": "2"},
        )
        message_id = await sqs_client.enqueue(url, b'{"value":"keep-me"}')

        first = (await _receive_until(sqs_client, url))[0]
        assert await sqs_client.nack(url, [first.id], requeue=False) == 1

        # Not redelivered immediately: requeue=False does not reset visibility.
        assert await sqs_client.receive(url, limit=1) == []

        # ... but it reappears after the visibility timeout (no deletion).
        await asyncio.sleep(2.5)
        again = (await _receive_until(sqs_client, url))[0]
        assert again.id == message_id
        assert again.body == b'{"value":"keep-me"}'
        assert await sqs_client.ack(url, [again.id]) == 1


@pytest.mark.asyncio
async def test_client_health_without_ambient_scope(
    sqs_client: SQSClient,
) -> None:
    """health() opens its own client scope when none is active."""

    message, ok = await sqs_client.health()

    assert ok is True
    assert message == "ok"


@pytest.mark.asyncio
async def test_client_delayed_enqueue_not_visible_until_delay_elapses(
    sqs_client: SQSClient,
    sqs_queue_url: str,
) -> None:
    async with sqs_client.client():
        await sqs_client.enqueue(
            sqs_queue_url,
            b'{"value":"delayed"}',
            delay=timedelta(seconds=2),
        )

        immediate = await sqs_client.receive(sqs_queue_url, limit=1)
        assert immediate == []

        messages = await _receive_until(sqs_client, sqs_queue_url, attempts=12)
        assert messages[0].body == b'{"value":"delayed"}'
        await sqs_client.ack(sqs_queue_url, [messages[0].id])


@pytest.mark.asyncio
async def test_sequential_operations_reuse_single_aiobotocore_client(
    floci_container,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``initialize`` opens one aiobotocore client; sequential ops reuse it.

    The emulator fixture passes explicit static credentials, so the
    credential-chain (``access_key_id=None``) path is exercised by unit
    tests only.
    """
    import aioboto3

    create_calls = 0
    original_client = aioboto3.Session.client

    def _counting_client(self, *args, **kwargs):
        nonlocal create_calls
        create_calls += 1
        return original_client(self, *args, **kwargs)

    monkeypatch.setattr(aioboto3.Session, "client", _counting_client)

    client = SQSClient()
    await client.initialize(
        endpoint=floci_container.get_url(),
        region_name="us-east-1",
        access_key_id="test",
        secret_access_key="test",
    )

    try:
        queue = f"forze-sqs-reuse-{uuid4().hex[:12]}"

        async with client.client():
            queue_url = await client.create_queue(queue)

        async with client.client():
            message_id = await client.enqueue(queue_url, b"reuse-payload")
            assert message_id

        async with client.client():
            messages = await _receive_until(client, queue_url)
            assert messages[0].body == b"reuse-payload"
            assert await client.ack(queue_url, [messages[0].id]) == 1

        assert create_calls == 1

    finally:
        await client.close()


@pytest.mark.asyncio
async def test_client_resolves_region_from_environment(
    floci_container,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``region_name=None`` defers to the chain: env var alone is enough."""

    monkeypatch.delenv("AWS_REGION", raising=False)
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")

    client = SQSClient()
    await client.initialize(
        endpoint=floci_container.get_url(),
        access_key_id="test",
        secret_access_key="test",
        # no region_name: botocore resolves it from AWS_DEFAULT_REGION
    )

    try:
        async with client.client():
            queue = f"forze-sqs-envregion-{uuid4().hex[:12]}"
            await client.create_queue(queue)

            message_id = await client.enqueue(queue, b"region-from-env")
            assert message_id

            messages = await _receive_until(client, queue)
            assert messages[0].body == b"region-from-env"
            assert await client.ack(queue, [messages[0].id]) == 1

    finally:
        await client.close()
