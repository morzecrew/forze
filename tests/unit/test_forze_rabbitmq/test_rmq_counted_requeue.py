"""Counted requeue (``redelivery_counting``) isolates a per-message republish failure: only the
originals whose republished copy reached the broker are acked. A failed-republish original is left
**unacked** for broker redelivery — acking it would drop the message (the copy never landed)."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any
from unittest.mock import AsyncMock

import pytest

pytest.importorskip("aio_pika")

from forze_rabbitmq.kernel.client.client import RabbitMQClient
from forze_rabbitmq.kernel.client.value_objects import RabbitMQConfig

pytestmark = pytest.mark.unit


class _FakeExchange:
    def __init__(self, fail_bodies: frozenset[bytes]) -> None:
        self.fail_bodies = fail_bodies
        self.published: list[bytes] = []

    async def publish(self, message: Any, routing_key: str) -> None:
        self.published.append(message.body)
        if message.body in self.fail_bodies:
            raise RuntimeError("broker rejected publish")


class _FakeChannel:
    is_closed = False

    def __init__(self, exchange: _FakeExchange) -> None:
        self.default_exchange = exchange


class _FakeRaw:
    def __init__(self, message_id: str, body: bytes) -> None:
        self.message_id = message_id
        self.body = body
        self.content_type = "application/json"
        self.content_encoding = None
        self.priority = None
        self.correlation_id = None
        self.reply_to = None
        self.expiration = None
        self.timestamp = None
        self.type = None
        self.user_id = None
        self.app_id = None
        self.headers: dict[str, Any] = {}
        self.acked = False

    async def ack(self) -> None:
        self.acked = True


def _client(exchange: _FakeExchange) -> RabbitMQClient:
    client = RabbitMQClient()
    client._RabbitMQClient__config = RabbitMQConfig(  # type: ignore[attr-defined]
        redelivery_counting=True
    )
    client._RabbitMQClient__declare_queue = AsyncMock()  # type: ignore[attr-defined]

    channel = _FakeChannel(exchange)

    @asynccontextmanager
    async def _channel() -> AsyncIterator[_FakeChannel]:
        yield channel

    client.channel = _channel  # type: ignore[method-assign, assignment]
    return client


@pytest.mark.asyncio
async def test_partial_publish_failure_acks_only_republished_originals() -> None:
    exchange = _FakeExchange(fail_bodies=frozenset({b"bad"}))
    client = _client(exchange)

    good, bad = _FakeRaw("good", b"good"), _FakeRaw("bad", b"bad")

    # Must not raise despite one publish failing.
    await client._RabbitMQClient__requeue_counted("q", [good, bad])  # type: ignore[attr-defined]

    # Both were attempted; only the successfully-republished original is acked.
    assert set(exchange.published) == {b"good", b"bad"}
    assert good.acked is True
    assert bad.acked is False  # left unacked -> broker redelivers it (no loss)


@pytest.mark.asyncio
async def test_all_publish_success_acks_every_original() -> None:
    exchange = _FakeExchange(fail_bodies=frozenset())
    client = _client(exchange)

    raws = [_FakeRaw(f"m{i}", f"m{i}".encode()) for i in range(3)]
    await client._RabbitMQClient__requeue_counted("q", raws)  # type: ignore[attr-defined]

    assert all(raw.acked for raw in raws)
