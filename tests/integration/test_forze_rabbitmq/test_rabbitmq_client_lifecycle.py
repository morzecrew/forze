"""RabbitMQ platform client lifecycle and health."""

from __future__ import annotations

import pytest

from forze_rabbitmq.kernel.client import RabbitMQClient


@pytest.mark.integration
@pytest.mark.asyncio
async def test_rabbitmq_health_without_initialize() -> None:
    client = RabbitMQClient()
    msg, ok = await client.health()
    assert ok is False
    assert msg


@pytest.mark.integration
@pytest.mark.asyncio
async def test_rabbitmq_close_without_initialize_is_noop() -> None:
    client = RabbitMQClient()
    await client.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_rabbitmq_health_when_connected(rabbitmq_client: RabbitMQClient) -> None:
    msg, ok = await rabbitmq_client.health()
    assert ok is True
    assert msg == "ok"
