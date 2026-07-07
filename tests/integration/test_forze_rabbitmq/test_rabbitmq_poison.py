"""Poison / redelivery hardening against a real broker.

- An opt-in ``dead_letter_exchange`` makes ``nack(requeue=False)`` dead-letter a rejected message
  to ``<dlx>.dlq`` instead of silently dropping it.
- Opt-in ``redelivery_counting`` advances the delivery count past the broker's ``redelivered``-flag
  ceiling of 2 (via a republished ``x-forze-delivery`` header), so ``max_deliveries >= 2``
  poison-parking can actually fire; the message id is preserved for consumer inbox dedup.
"""

from datetime import timedelta
from urllib.parse import quote
from uuid import uuid4

import pytest
from testcontainers.rabbitmq import RabbitMqContainer

from forze_rabbitmq.kernel.client import RabbitMQClient, RabbitMQConfig

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


def _dsn(container: RabbitMqContainer) -> str:
    host = container.get_container_host_ip()
    port = container.get_exposed_port(container.port)
    vhost = quote(container.vhost, safe="")
    return f"amqp://{container.username}:{container.password}@{host}:{port}/{vhost}"


async def _client(container: RabbitMqContainer, **config_kw: object) -> RabbitMQClient:
    client = RabbitMQClient()
    await client.initialize(
        dsn=_dsn(container),
        config=RabbitMQConfig(
            prefetch_count=20,
            connect_timeout=timedelta(seconds=10.0),
            **config_kw,  # type: ignore[arg-type]
        ),
    )
    return client


async def test_dead_letter_exchange_captures_rejected_message(
    rabbitmq_container: RabbitMqContainer,
    rabbitmq_client: RabbitMQClient,
) -> None:
    dlx = f"it-forze-dlx-{uuid4().hex[:8]}"
    dlx_client = await _client(rabbitmq_container, dead_letter_exchange=dlx)

    try:
        queue = f"it:rmq:dlx:{uuid4().hex[:8]}"
        await dlx_client.enqueue(queue, b'{"value":"poison"}')

        [msg] = await dlx_client.receive(queue, limit=1, timeout=timedelta(seconds=2))
        # Reject without requeue -> dead-letter, not drop.
        assert await dlx_client.nack(queue, [msg.id], requeue=False) == 1

        # The rejected message lands in <dlx>.dlq. Read it with the plain client (no DLX config)
        # so its declare of the DLQ doesn't conflict with the arg-less DLQ declaration.
        dlq = f"{dlx}.dlq"
        [dead] = await rabbitmq_client.receive(dlq, limit=1, timeout=timedelta(seconds=5))
        assert dead.body == b'{"value":"poison"}'
        await rabbitmq_client.ack(dlq, [dead.id])

    finally:
        await dlx_client.close()


async def test_redelivery_counting_advances_past_two(
    rabbitmq_container: RabbitMqContainer,
) -> None:
    client = await _client(rabbitmq_container, redelivery_counting=True)

    try:
        queue = f"it:rmq:count:{uuid4().hex[:8]}"
        await client.enqueue(queue, b'{"value":"retry"}')

        [m1] = await client.receive(queue, limit=1, timeout=timedelta(seconds=2))
        assert m1.delivery_count == 1
        assert await client.nack(queue, [m1.id], requeue=True) == 1

        [m2] = await client.receive(queue, limit=1, timeout=timedelta(seconds=3))
        assert m2.id == m1.id  # id preserved -> inbox dedup still collapses copies
        assert m2.delivery_count == 2  # advanced (a plain requeue would still report 2)
        assert await client.nack(queue, [m2.id], requeue=True) == 1

        [m3] = await client.receive(queue, limit=1, timeout=timedelta(seconds=3))
        # Past the redelivered-flag ceiling of 2 — so max_deliveries=2 parking would now fire.
        assert m3.delivery_count == 3
        await client.ack(queue, [m3.id])

    finally:
        await client.close()
