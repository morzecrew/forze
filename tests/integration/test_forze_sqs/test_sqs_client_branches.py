"""Branch-coverage integration tests for :class:`SQSClient`.

Exercises queue-resolution, batch send/ack/nack, attribute extraction,
empty-input fast paths, and failure paths against the SQS emulator.
"""

from __future__ import annotations

from datetime import timedelta
from uuid import uuid4

import pytest

from forze.base.exceptions import CoreException
from forze_sqs.kernel.client import SQSClient
from forze_sqs.kernel.client.value_objects import SQSConfig

pytestmark = pytest.mark.integration


async def _receive_until(client: SQSClient, queue: str, *, attempts: int = 8):
    for _ in range(attempts):
        messages = await client.receive(queue, limit=1, timeout=timedelta(seconds=1))
        if messages:
            return messages
    raise AssertionError("SQS message was not received in time")


@pytest.mark.asyncio
async def test_empty_input_fast_paths(
    sqs_client: SQSClient,
    sqs_queue_url: str,
) -> None:
    async with sqs_client.client():
        assert await sqs_client.enqueue_many(sqs_queue_url, []) == []
        assert await sqs_client.ack(sqs_queue_url, []) == 0
        assert await sqs_client.nack(sqs_queue_url, []) == 0
        # receive with non-positive limit short-circuits.
        assert await sqs_client.receive(sqs_queue_url, limit=0) == []


@pytest.mark.asyncio
async def test_enqueue_many_message_ids_mismatch(
    sqs_client: SQSClient,
    sqs_queue_url: str,
) -> None:
    async with sqs_client.client():
        with pytest.raises(CoreException, match="message_ids size"):
            await sqs_client.enqueue_many(
                sqs_queue_url,
                [b"a", b"b"],
                message_ids=["only-one"],
            )


@pytest.mark.asyncio
async def test_create_queue_returns_url_when_passed_url(
    sqs_client: SQSClient,
    sqs_queue_url: str,
) -> None:
    async with sqs_client.client():
        # A URL passed to create_queue is returned verbatim (is_queue_url branch).
        assert await sqs_client.create_queue(sqs_queue_url) == sqs_queue_url


@pytest.mark.asyncio
async def test_create_queue_with_attributes(
    sqs_client: SQSClient,
) -> None:
    queue = f"forze-attr-{uuid4().hex[:10]}"
    async with sqs_client.client():
        url = await sqs_client.create_queue(
            queue,
            attributes={"DelaySeconds": "0", "VisibilityTimeout": "30"},
        )
        assert url.endswith(queue) or queue.replace("-", "_") in url


@pytest.mark.asyncio
async def test_queue_url_resolution_and_cache(
    sqs_client: SQSClient,
) -> None:
    queue = f"forze-resolve-{uuid4().hex[:10]}"
    async with sqs_client.client():
        created = await sqs_client.create_queue(queue)
        # First resolve populates/uses cache; passing a URL returns verbatim.
        assert await sqs_client.queue_url(created) == created
        # Resolving by name hits the cache populated during create_queue.
        resolved = await sqs_client.queue_url(queue)
        assert resolved == created


@pytest.mark.asyncio
async def test_queue_url_resolves_via_get_queue_url(
    sqs_client: SQSClient,
) -> None:
    """A fresh client resolves an existing queue through get_queue_url."""

    queue = f"forze-geturl-{uuid4().hex[:10]}"
    async with sqs_client.client():
        created = await sqs_client.create_queue(queue)

    # Second client has an empty cache, forcing the get_queue_url path.
    other = SQSClient()
    await sqs_client_clone_init(sqs_client, other)
    async with other.client():
        resolved = await other.queue_url(queue)
        assert resolved.rstrip("/").endswith(created.rstrip("/").rsplit("/", 1)[-1])
    await other.close()


async def sqs_client_clone_init(src: SQSClient, dst: SQSClient) -> None:
    """Initialize *dst* against the same endpoint/credentials as *src*."""

    opts = src._SQSClient__opts  # type: ignore[attr-defined]
    await dst.initialize(
        endpoint=opts.endpoint,
        region_name=opts.region_name,
        access_key_id=opts.access_key_id,
        secret_access_key=opts.secret_access_key,
    )


@pytest.mark.asyncio
async def test_zero_delay_is_dropped(
    sqs_client: SQSClient,
    sqs_queue_url: str,
) -> None:
    async with sqs_client.client():
        # A zero/negative delay resolves to None (no DelaySeconds entry).
        message_id = await sqs_client.enqueue(
            sqs_queue_url,
            b"no-delay",
            delay=timedelta(seconds=0),
        )
        assert message_id
        messages = await _receive_until(sqs_client, sqs_queue_url)
        assert messages[0].body == b"no-delay"
        await sqs_client.ack(sqs_queue_url, [messages[0].id])


@pytest.mark.asyncio
async def test_subsecond_delay_rounds_down_to_none(
    sqs_client: SQSClient,
    sqs_queue_url: str,
) -> None:
    """A sub-second delay resolves non-None but rounds to 0 seconds -> dropped."""

    async with sqs_client.client():
        message_id = await sqs_client.enqueue(
            sqs_queue_url,
            b"subsec",
            delay=timedelta(milliseconds=500),
        )
        assert message_id
        messages = await _receive_until(sqs_client, sqs_queue_url)
        assert messages[0].body == b"subsec"
        await sqs_client.ack(sqs_queue_url, [messages[0].id])


@pytest.mark.asyncio
async def test_resolve_by_sanitized_name_hits_cache(
    sqs_client: SQSClient,
) -> None:
    """Resolving by a name that sanitizes to the cached key hits the name cache."""

    # Name with characters that get sanitized differently from the raw key.
    raw = f"forze.san {uuid4().hex[:8]}"
    async with sqs_client.client():
        created = await sqs_client.create_queue(raw)
        # Re-create caches both raw and sanitized keys; resolving the sanitized
        # form directly exercises the queue_name cache branch.
        sanitized = SQSClient._SQSClient__sanitize_queue_name(raw)  # type: ignore[attr-defined]
        assert await sqs_client.queue_url(sanitized) == created


@pytest.mark.asyncio
async def test_resolve_alias_name_hits_sanitized_cache(
    sqs_client: SQSClient,
) -> None:
    """A distinct raw name that sanitizes to a cached key hits the name cache."""

    token = uuid4().hex[:8]
    raw = f"forze.alias.{token}"
    # A different raw spelling collapsing to the same sanitized name.
    alias = f"forze!alias!{token}"
    assert (
        SQSClient._SQSClient__sanitize_queue_name(raw)  # type: ignore[attr-defined]
        == SQSClient._SQSClient__sanitize_queue_name(alias)  # type: ignore[attr-defined]
    )
    async with sqs_client.client():
        created = await sqs_client.create_queue(raw)
        # ``alias`` itself is not cached, but its sanitized form is.
        assert await sqs_client.queue_url(alias) == created


@pytest.mark.asyncio
async def test_nested_client_reuses_parent(
    sqs_client: SQSClient,
    sqs_queue_url: str,
) -> None:
    """A nested ``client()`` scope reuses the parent context client."""

    async with sqs_client.client() as outer:
        async with sqs_client.client() as inner:
            assert inner is outer
            _, ok = await sqs_client.health()
            assert ok is True


@pytest.mark.asyncio
async def test_consume_skips_empty_polls(
    sqs_client: SQSClient,
) -> None:
    """consume() loops past empty receives before yielding a message."""

    import asyncio

    queue = f"forze-consume-{uuid4().hex[:10]}"
    async with sqs_client.client():
        await sqs_client.create_queue(queue)
        # Short long-poll so an initially-empty queue yields an empty receive,
        # exercising the ``continue`` branch before the message lands.
        stream = sqs_client.consume(queue, timeout=timedelta(seconds=0))

        async def _delayed_enqueue() -> None:
            await asyncio.sleep(1.5)
            await sqs_client.enqueue(queue, b"eventually")

        producer = asyncio.create_task(_delayed_enqueue())
        try:
            message = await asyncio.wait_for(anext(stream), timeout=15)
        finally:
            await producer
            await stream.aclose()
        assert message.body == b"eventually"
        await sqs_client.ack(queue, [message.id])


@pytest.mark.asyncio
async def test_enqueue_delay_over_max_raises(
    sqs_client: SQSClient,
    sqs_queue_url: str,
) -> None:
    """A delay beyond the 900s SQS limit raises a precondition error."""

    async with sqs_client.client():
        with pytest.raises(CoreException, match="900 seconds"):
            await sqs_client.enqueue(
                sqs_queue_url,
                b"too-late",
                delay=timedelta(seconds=1000),
            )


@pytest.mark.asyncio
async def test_enqueue_many_multiple_chunks_concurrent(
    sqs_client: SQSClient,
) -> None:
    """More than one batch chunk exercises the concurrent TaskGroup path."""

    queue = f"forze-bulk-{uuid4().hex[:10]}"
    async with sqs_client.client():
        await sqs_client.create_queue(queue)
        bodies = [f"m{i}".encode() for i in range(15)]  # > 10 -> 2 chunks
        ids = await sqs_client.enqueue_many(queue, bodies)
        assert len(ids) == 15

        received: list[bytes] = []
        for _ in range(30):
            msgs = await sqs_client.receive(queue, limit=10, timeout=timedelta(seconds=1))
            received.extend(m.body for m in msgs)
            if msgs:
                await sqs_client.ack(queue, [m.id for m in msgs])
            if len(received) >= 15:
                break
        assert len(received) == 15


@pytest.mark.asyncio
async def test_fifo_enqueue_and_receive(
    sqs_client: SQSClient,
) -> None:
    """FIFO queues exercise MessageGroupId/MessageDeduplicationId entry fields."""

    queue = f"forze-fifo-{uuid4().hex[:10]}.fifo"
    async with sqs_client.client():
        url = await sqs_client.create_queue(
            queue,
            attributes={"FifoQueue": "true", "ContentBasedDeduplication": "false"},
        )
        await sqs_client.enqueue(url, b"fifo-body", key="group-1")
        messages = await _receive_until(sqs_client, url)
        assert messages[0].body == b"fifo-body"
        await sqs_client.ack(url, [messages[0].id])


@pytest.mark.asyncio
async def test_enqueued_at_falls_back_to_sent_timestamp(
    sqs_client: SQSClient,
) -> None:
    """When no forze_enqueued_at attribute is set, SentTimestamp is used."""

    queue = f"forze-ts-{uuid4().hex[:10]}"
    async with sqs_client.client():
        await sqs_client.create_queue(queue)
        await sqs_client.enqueue(queue, b"ts-body")  # no enqueued_at
        messages = await _receive_until(sqs_client, queue)
        msg = messages[0]
        assert msg.enqueued_at is not None
        assert msg.enqueued_at.tzinfo is not None
        await sqs_client.ack(queue, [msg.id])


@pytest.mark.asyncio
async def test_ack_unknown_id_returns_zero(
    sqs_client: SQSClient,
    sqs_queue_url: str,
) -> None:
    """Acking an id with no pending delivery on this client is a no-op."""

    async with sqs_client.client():
        assert await sqs_client.ack(sqs_queue_url, ["never-received-id"]) == 0


@pytest.mark.asyncio
async def test_nack_without_requeue_does_not_delete(
    sqs_client: SQSClient,
    sqs_queue_url: str,
) -> None:
    """nack(requeue=False) leaves the message in the queue (invisible until
    its visibility timeout) instead of deleting it."""

    async with sqs_client.client():
        await sqs_client.enqueue(sqs_queue_url, b"keep-me")
        msg = (await _receive_until(sqs_client, sqs_queue_url))[0]
        assert await sqs_client.nack(sqs_queue_url, [msg.id], requeue=False) == 1
        # Still owned by the original receive's visibility timeout, so not
        # visible — but crucially never deleted (see test_sqs_client.py for
        # the reappearance assertion on a short-visibility queue).
        assert await sqs_client.receive(sqs_queue_url, limit=1) == []


@pytest.mark.asyncio
async def test_nack_unknown_id_returns_zero(
    sqs_client: SQSClient,
    sqs_queue_url: str,
) -> None:
    """Nacking an id with no pending delivery on this client is a no-op."""

    async with sqs_client.client():
        assert (
            await sqs_client.nack(
                sqs_queue_url,
                ["never-received-id"],
                requeue=True,
            )
            == 0
        )


@pytest.mark.asyncio
async def test_health_ok(
    sqs_client: SQSClient,
    sqs_queue_url: str,
) -> None:
    async with sqs_client.client():
        message, ok = await sqs_client.health()
        assert ok is True
        assert message == "ok"


@pytest.mark.asyncio
async def test_client_options_missing_raises() -> None:
    """Entering client() with a session but no opts raises internal.

    After ``initialize`` the persistent client would be yielded without
    consulting opts, so it is dropped too: the lazy fallback path is the
    only one that still builds a client from opts.
    """

    client = SQSClient()
    await client.initialize(
        endpoint="http://localhost:4566",
        region_name="us-east-1",
        access_key_id="test",
        secret_access_key="test",
    )
    # Drop opts and the persistent client while keeping the session to hit
    # the lazy-construction guard branch.
    client._SQSClient__opts = None  # type: ignore[attr-defined]
    client._SQSClient__persistent_client = None  # type: ignore[attr-defined]
    with pytest.raises(CoreException):
        async with client.client():
            pass
    await client.close()


@pytest.mark.asyncio
async def test_initialize_with_config_pool_size(
    floci_container,
) -> None:
    """Passing a config with max_pool_connections sizes batch concurrency."""

    endpoint = floci_container.get_url()
    client = SQSClient()
    await client.initialize(
        endpoint=endpoint,
        region_name="us-east-1",
        access_key_id="test",
        secret_access_key="test",
        config=SQSConfig(max_pool_connections=4),
    )
    async with client.client():
        _, ok = await client.health()
        assert ok is True
    await client.close()


@pytest.mark.asyncio
async def test_health_reports_failure_on_broken_endpoint() -> None:
    """health() catches the underlying list_queues error and returns False."""

    client = SQSClient()
    await client.initialize(
        endpoint="http://127.0.0.1:1",  # nothing listening
        region_name="us-east-1",
        access_key_id="test",
        secret_access_key="test",
        config=SQSConfig(
            connect_timeout=timedelta(seconds=1),
            read_timeout=timedelta(seconds=1),
        ),
    )
    async with client.client():
        message, ok = await client.health()
        assert ok is False
        assert message
    await client.close()


@pytest.mark.asyncio
async def test_initialize_is_idempotent(
    floci_container,
) -> None:
    endpoint = floci_container.get_url()
    client = SQSClient()
    await client.initialize(
        endpoint=endpoint,
        region_name="us-east-1",
        access_key_id="test",
        secret_access_key="test",
    )
    # Second call must early-return without replacing the session.
    await client.initialize(
        endpoint=endpoint,
        region_name="us-east-1",
        access_key_id="test",
        secret_access_key="test",
    )
    async with client.client():
        _, ok = await client.health()
        assert ok is True
    await client.close()
