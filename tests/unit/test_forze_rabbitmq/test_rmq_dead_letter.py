"""The DLX topology is declared once per client (broker-global + durable), not on every
work-queue declaration — the ``__dead_letter_ready`` guard collapses the repeated
declare_exchange/declare_queue/bind round-trips, and a teardown resets it so a fresh connection
re-declares."""

from unittest.mock import AsyncMock, MagicMock

import pytest

pytest.importorskip("aio_pika")

from forze_rabbitmq.kernel.client.client import RabbitMQClient
from forze_rabbitmq.kernel.client.value_objects import RabbitMQConfig

pytestmark = pytest.mark.unit


def _mock_channel() -> AsyncMock:
    channel = AsyncMock()
    dlq = MagicMock()
    dlq.bind = AsyncMock()
    channel.declare_exchange = AsyncMock(return_value=MagicMock())
    channel.declare_queue = AsyncMock(return_value=dlq)
    return channel


def _client(**config_kw: object) -> RabbitMQClient:
    client = RabbitMQClient()
    client._RabbitMQClient__config = RabbitMQConfig(**config_kw)  # type: ignore[attr-defined, arg-type]
    return client


@pytest.mark.asyncio
async def test_dead_letter_declared_once_per_client() -> None:
    client = _client(dead_letter_exchange="dlx")
    channel = _mock_channel()
    ensure = client._RabbitMQClient__ensure_dead_letter  # type: ignore[attr-defined]

    await ensure(channel)
    await ensure(channel)
    await ensure(channel)

    # Three calls, one declaration each of exchange/queue/bind.
    assert channel.declare_exchange.await_count == 1
    assert channel.declare_queue.await_count == 1
    assert channel.declare_queue.return_value.bind.await_count == 1


@pytest.mark.asyncio
async def test_teardown_reset_forces_redeclare() -> None:
    client = _client(dead_letter_exchange="dlx")
    channel = _mock_channel()
    ensure = client._RabbitMQClient__ensure_dead_letter  # type: ignore[attr-defined]

    await ensure(channel)
    # Simulate a teardown clearing the guard (a fresh connection must re-declare).
    client._RabbitMQClient__dead_letter_ready = False  # type: ignore[attr-defined]
    await ensure(channel)

    assert channel.declare_exchange.await_count == 2


@pytest.mark.asyncio
async def test_no_dlx_configured_declares_nothing() -> None:
    client = _client()  # dead_letter_exchange defaults None
    channel = _mock_channel()

    await client._RabbitMQClient__ensure_dead_letter(channel)  # type: ignore[attr-defined]

    channel.declare_exchange.assert_not_awaited()
    channel.declare_queue.assert_not_awaited()
