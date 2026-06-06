"""Branch-coverage integration tests for :class:`RabbitMQClient`.

Covers lifecycle guards, nested channel reuse, delay-queue handling,
empty/no-match fast paths for ack/nack, and pending-id collision handling.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from urllib.parse import quote
from uuid import uuid4

import pytest
from pydantic import SecretStr

from forze.base.exceptions import CoreException
from forze_rabbitmq.kernel.client import RabbitMQClient

pytestmark = pytest.mark.integration


def _dsn(container) -> str:
    host = container.get_container_host_ip()
    port = container.get_exposed_port(container.port)
    vhost = quote(container.vhost, safe="")
    return f"amqp://{container.username}:{container.password}@{host}:{port}/{vhost}"


async def _receive_until(client: RabbitMQClient, queue: str, *, attempts: int = 8):
    for _ in range(attempts):
        messages = await client.receive(queue, limit=1, timeout=timedelta(seconds=1))
        if messages:
            return messages
    raise AssertionError("RabbitMQ message was not received in time")


async def _receive_exact(client: RabbitMQClient, queue: str, expected: int, *, attempts: int = 12):
    out: list = []
    for _ in range(attempts):
        remaining = expected - len(out)
        if remaining <= 0:
            return out
        msgs = await client.receive(queue, limit=remaining, timeout=timedelta(seconds=1))
        out.extend(msgs)
    if len(out) == expected:
        return out
    raise AssertionError("RabbitMQ batch was not received in time")


@pytest.mark.asyncio
async def test_initialize_with_secret_dsn_and_idempotent(
    rabbitmq_container,
) -> None:
    """A SecretStr DSN is unwrapped and a second initialize is a no-op."""

    client = RabbitMQClient()
    await client.initialize(dsn=SecretStr(_dsn(rabbitmq_container)))
    # Second initialize while the connection is open early-returns.
    await client.initialize(dsn=SecretStr(_dsn(rabbitmq_container)))
    _, ok = await client.health()
    assert ok is True
    await client.close()


@pytest.mark.asyncio
async def test_empty_input_fast_paths(
    rabbitmq_client: RabbitMQClient,
) -> None:
    queue = f"it:empty:{uuid4().hex[:10]}"
    assert await rabbitmq_client.enqueue_many(queue, []) == []
    assert await rabbitmq_client.ack(queue, []) == 0
    assert await rabbitmq_client.nack(queue, []) == 0
    assert await rabbitmq_client.receive(queue, limit=0) == []


@pytest.mark.asyncio
async def test_enqueue_many_message_ids_mismatch(
    rabbitmq_client: RabbitMQClient,
) -> None:
    queue = f"it:mismatch:{uuid4().hex[:10]}"
    with pytest.raises(CoreException, match="message_ids size"):
        await rabbitmq_client.enqueue_many(
            queue,
            [b"a", b"b"],
            message_ids=["only-one"],
        )


@pytest.mark.asyncio
async def test_ack_nack_unknown_ids_return_zero(
    rabbitmq_client: RabbitMQClient,
) -> None:
    """ack/nack with ids that are not pending return 0 (no-match branch)."""

    queue = f"it:unknown:{uuid4().hex[:10]}"
    assert await rabbitmq_client.ack(queue, ["nope"]) == 0
    assert await rabbitmq_client.nack(queue, ["nope"], requeue=False) == 0


@pytest.mark.asyncio
async def test_ack_ignores_wrong_queue(
    rabbitmq_client: RabbitMQClient,
) -> None:
    """A pending id registered under one queue is not acked under another."""

    queue = f"it:wq:{uuid4().hex[:10]}"
    other = f"it:wq-other:{uuid4().hex[:10]}"
    await rabbitmq_client.enqueue(queue, b'{"value":"x"}')
    msg = (await _receive_until(rabbitmq_client, queue))[0]
    # Same id but different queue -> filtered out -> 0.
    assert await rabbitmq_client.ack(other, [msg.id]) == 0
    # Correct queue still works.
    assert await rabbitmq_client.ack(queue, [msg.id]) == 1


@pytest.mark.asyncio
async def test_duplicate_message_ids_get_suffixed(
    rabbitmq_client: RabbitMQClient,
) -> None:
    """Receiving two messages with the same message_id yields distinct ids."""

    queue = f"it:dup:{uuid4().hex[:10]}"
    await rabbitmq_client.enqueue_many(
        queue,
        [b'{"value":"d1"}', b'{"value":"d2"}'],
        message_ids=["same-id", "same-id"],
    )
    messages = await _receive_exact(rabbitmq_client, queue, expected=2)
    ids = {m.id for m in messages}
    assert len(ids) == 2  # one was suffixed
    assert "same-id" in ids
    assert await rabbitmq_client.ack(queue, list(ids)) == 2


@pytest.mark.asyncio
async def test_duplicate_message_ids_single_receive_suffixed(
    rabbitmq_client: RabbitMQClient,
) -> None:
    """Single-message receive path also suffixes a colliding id."""

    queue = f"it:dup1:{uuid4().hex[:10]}"
    await rabbitmq_client.enqueue(queue, b'{"value":"a"}', message_id="collide")
    await rabbitmq_client.enqueue(queue, b'{"value":"b"}', message_id="collide")

    first = (await _receive_until(rabbitmq_client, queue))[0]
    second = (await _receive_until(rabbitmq_client, queue))[0]
    assert first.id != second.id
    assert {first.id, second.id} >= {"collide"}
    assert await rabbitmq_client.ack(queue, [first.id, second.id]) == 2


@pytest.mark.asyncio
async def test_nested_channel_reuses_parent(
    rabbitmq_client: RabbitMQClient,
) -> None:
    async with rabbitmq_client.channel() as outer:
        async with rabbitmq_client.channel() as inner:
            assert inner is outer


@pytest.mark.asyncio
async def test_delayed_enqueue_uses_delay_queue_and_caches(
    rabbitmq_client: RabbitMQClient,
) -> None:
    """Delayed enqueue declares the DLX delay queue once, then reuses it."""

    queue = f"it:delay:{uuid4().hex[:10]}"
    # First delayed publish declares the delay queue.
    await rabbitmq_client.enqueue(
        queue,
        b'{"value":"d1"}',
        delay=timedelta(seconds=1),
        delayed_delivery=True,
    )
    # Second publish hits the delay-queue-ready cache branch.
    await rabbitmq_client.enqueue(
        queue,
        b'{"value":"d2"}',
        delay=timedelta(seconds=1),
        delayed_delivery=True,
    )
    messages = await _receive_exact(rabbitmq_client, queue, expected=2)
    assert {m.body for m in messages} == {b'{"value":"d1"}', b'{"value":"d2"}'}
    assert await rabbitmq_client.ack(queue, [m.id for m in messages]) == 2


@pytest.mark.asyncio
async def test_delayed_enqueue_requires_delayed_delivery_flag(
    rabbitmq_client: RabbitMQClient,
) -> None:
    """A delay without delayed_delivery=True raises a precondition error."""

    queue = f"it:nodelayflag:{uuid4().hex[:10]}"
    with pytest.raises(CoreException, match="delayed_delivery=True"):
        await rabbitmq_client.enqueue(
            queue,
            b'{"value":"x"}',
            delay=timedelta(seconds=1),
            delayed_delivery=False,
        )


@pytest.mark.asyncio
async def test_delay_over_max_expiration_raises(
    rabbitmq_client: RabbitMQClient,
) -> None:
    """A delay beyond the wire-max message expiration raises a precondition."""

    queue = f"it:overmax:{uuid4().hex[:10]}"
    with pytest.raises(CoreException, match="maximum message expiration"):
        await rabbitmq_client.enqueue(
            queue,
            b'{"value":"x"}',
            delay=timedelta(days=60),  # > 2**32-1 ms
            delayed_delivery=True,
        )


@pytest.mark.asyncio
async def test_consume_suffixes_duplicate_message_ids(
    rabbitmq_client: RabbitMQClient,
) -> None:
    """consume() single-message path suffixes a colliding pending id."""

    queue = f"it:consume-dup:{uuid4().hex[:10]}"
    await rabbitmq_client.enqueue(queue, b'{"value":"c1"}', message_id="dupc")
    await rabbitmq_client.enqueue(queue, b'{"value":"c2"}', message_id="dupc")

    stream = rabbitmq_client.consume(queue, timeout=timedelta(seconds=2))
    first = await asyncio.wait_for(anext(stream), timeout=10)
    second = await asyncio.wait_for(anext(stream), timeout=10)
    await stream.aclose()

    assert first.id != second.id
    assert {first.id, second.id} >= {"dupc"}
    await rabbitmq_client.ack(queue, [first.id, second.id])


@pytest.mark.asyncio
async def test_subsecond_delay_rounds_to_immediate(
    rabbitmq_client: RabbitMQClient,
) -> None:
    """A sub-millisecond delay rounds down to 0 -> no expiration set."""

    queue = f"it:subms:{uuid4().hex[:10]}"
    await rabbitmq_client.enqueue(
        queue,
        b'{"value":"now"}',
        delay=timedelta(microseconds=100),  # < 1ms -> 0 ms
        delayed_delivery=True,
    )
    messages = await _receive_until(rabbitmq_client, queue)
    assert messages[0].body == b'{"value":"now"}'
    await rabbitmq_client.ack(queue, [messages[0].id])
