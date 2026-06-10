"""Unit tests for RabbitMQ delayed enqueue (per-delay-value DLX queues)."""

from __future__ import annotations

from datetime import timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

pytest.importorskip("aio_pika")

from forze.base.exceptions import CoreException
from forze_rabbitmq.kernel.client import RabbitMQClient
from forze_rabbitmq.kernel.client.client import (
    _DELAY_QUEUE_MAX_EXPIRES_GRACE_MS,
    _DELAY_QUEUE_MIN_EXPIRES_MS,
    _RABBITMQ_MAX_EXPIRATION_MS,
)

# ----------------------- #


def test_delay_queue_name_includes_delay_bucket() -> None:
    assert (
        RabbitMQClient._delay_queue_name("ns:jobs", 3000)
        == "ns:jobs.__forze_delay.3000"
    )
    assert (
        RabbitMQClient._delay_queue_name("ns:jobs", 600000)
        == "ns:jobs.__forze_delay.600000"
    )


# ....................... #


class TestDelayQueueExpires:
    def test_short_delay_floored_at_minimum(self) -> None:
        assert (
            RabbitMQClient._delay_queue_expires_ms(100)
            == _DELAY_QUEUE_MIN_EXPIRES_MS
        )

    def test_mid_delay_is_factor_of_ttl(self) -> None:
        assert RabbitMQClient._delay_queue_expires_ms(600_000) == 6_000_000

    def test_long_delay_capped_at_ttl_plus_grace(self) -> None:
        week_ms = 7 * 24 * 60 * 60 * 1000

        assert (
            RabbitMQClient._delay_queue_expires_ms(week_ms)
            == week_ms + _DELAY_QUEUE_MAX_EXPIRES_GRACE_MS
        )

    def test_never_exceeds_wire_bound(self) -> None:
        assert (
            RabbitMQClient._delay_queue_expires_ms(_RABBITMQ_MAX_EXPIRATION_MS)
            == _RABBITMQ_MAX_EXPIRATION_MS
        )

    def test_always_at_least_the_ttl(self) -> None:
        for delay_ms in (1, 100, 60_000, 600_000, _RABBITMQ_MAX_EXPIRATION_MS):
            assert RabbitMQClient._delay_queue_expires_ms(delay_ms) >= delay_ms


# ....................... #


def test_resolve_enqueue_delay_requires_config() -> None:
    with pytest.raises(CoreException):
        RabbitMQClient._resolve_enqueue_delay(
            delay=timedelta(seconds=5),
            not_before=None,
            delayed_delivery=False,
        )


def test_resolve_enqueue_delay_rejects_over_wire_bound() -> None:
    with pytest.raises(CoreException):
        RabbitMQClient._resolve_enqueue_delay(
            delay=timedelta(milliseconds=_RABBITMQ_MAX_EXPIRATION_MS + 1),
            not_before=None,
            delayed_delivery=True,
        )


# ....................... #


def _mock_channel() -> MagicMock:
    channel = AsyncMock()
    channel.declare_queue = AsyncMock(return_value=MagicMock())
    channel.default_exchange.publish = AsyncMock()

    return channel


def _patched_channel(client: RabbitMQClient, channel: MagicMock):
    cm = patch.object(client, "channel")
    started = cm.start()
    started.return_value.__aenter__ = AsyncMock(return_value=channel)
    started.return_value.__aexit__ = AsyncMock(return_value=None)

    return cm


@pytest.mark.asyncio
async def test_enqueue_many_publishes_to_per_delay_queue() -> None:
    client = RabbitMQClient()
    mock_channel = _mock_channel()

    cm = _patched_channel(client, mock_channel)

    try:
        await client.enqueue_many(
            "ns:jobs",
            [b"payload"],
            delay=timedelta(seconds=3),
            delayed_delivery=True,
        )
    finally:
        cm.stop()

    declare_calls = {
        call.args[0]: call.kwargs
        for call in mock_channel.declare_queue.await_args_list
    }

    assert "ns:jobs" in declare_calls
    assert "ns:jobs.__forze_delay.3000" in declare_calls

    arguments = declare_calls["ns:jobs.__forze_delay.3000"]["arguments"]
    assert arguments["x-message-ttl"] == 3000
    assert arguments["x-expires"] == _DELAY_QUEUE_MIN_EXPIRES_MS
    assert arguments["x-dead-letter-exchange"] == ""
    assert arguments["x-dead-letter-routing-key"] == "ns:jobs"

    publish_call = mock_channel.default_exchange.publish.await_args
    assert publish_call.kwargs["routing_key"] == "ns:jobs.__forze_delay.3000"

    # Queue-level TTL replaces per-message expiration (head-of-line safety).
    message = publish_call.args[0]
    assert message.expiration is None


@pytest.mark.asyncio
async def test_distinct_delays_use_distinct_queues_with_own_ttl() -> None:
    """A 10-minute delay and a 5-second delay never share a delay queue."""
    client = RabbitMQClient()
    mock_channel = _mock_channel()

    cm = _patched_channel(client, mock_channel)

    try:
        await client.enqueue(
            "ns:jobs",
            b"slow",
            delay=timedelta(minutes=10),
            delayed_delivery=True,
        )
        await client.enqueue(
            "ns:jobs",
            b"fast",
            delay=timedelta(seconds=5),
            delayed_delivery=True,
        )
    finally:
        cm.stop()

    declare_calls = {
        call.args[0]: call.kwargs
        for call in mock_channel.declare_queue.await_args_list
    }

    slow_args = declare_calls["ns:jobs.__forze_delay.600000"]["arguments"]
    fast_args = declare_calls["ns:jobs.__forze_delay.5000"]["arguments"]

    assert slow_args["x-message-ttl"] == 600000
    assert fast_args["x-message-ttl"] == 5000
    assert slow_args["x-expires"] == 6_000_000
    assert fast_args["x-expires"] == _DELAY_QUEUE_MIN_EXPIRES_MS

    routing_keys = [
        call.kwargs["routing_key"]
        for call in mock_channel.default_exchange.publish.await_args_list
    ]
    assert routing_keys == [
        "ns:jobs.__forze_delay.600000",
        "ns:jobs.__forze_delay.5000",
    ]


@pytest.mark.asyncio
async def test_delay_queue_redeclared_on_every_delayed_publish() -> None:
    """No declaration caching: x-expires may delete idle delay queues."""
    client = RabbitMQClient()
    mock_channel = _mock_channel()

    cm = _patched_channel(client, mock_channel)

    try:
        for _ in range(2):
            await client.enqueue(
                "ns:jobs",
                b"payload",
                delay=timedelta(seconds=3),
                delayed_delivery=True,
            )
    finally:
        cm.stop()

    delay_declares = [
        call
        for call in mock_channel.declare_queue.await_args_list
        if call.args[0] == "ns:jobs.__forze_delay.3000"
    ]
    assert len(delay_declares) == 2
