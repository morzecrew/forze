"""Unit tests for RabbitMQ delayed enqueue."""

from __future__ import annotations

from datetime import timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from forze.base.exceptions import CoreException
from forze_rabbitmq.kernel.client import RabbitMQClient


def test_delay_queue_name_suffix() -> None:
    assert RabbitMQClient._delay_queue_name("ns:jobs") == "ns:jobs.__forze_delay"


def test_resolve_message_expiration_requires_config() -> None:
    with pytest.raises(CoreException):
        RabbitMQClient._resolve_message_expiration(
            delay=timedelta(seconds=5),
            not_before=None,
            delayed_delivery=False,
        )


@pytest.mark.asyncio
async def test_enqueue_many_publishes_to_delay_queue_when_delayed() -> None:
    client = RabbitMQClient()
    mock_channel = AsyncMock()
    mock_channel.declare_queue = AsyncMock(return_value=MagicMock())
    mock_channel.default_exchange.publish = AsyncMock()

    with patch.object(client, "channel") as channel_cm:
        channel_cm.return_value.__aenter__ = AsyncMock(return_value=mock_channel)
        channel_cm.return_value.__aexit__ = AsyncMock(return_value=None)

        await client.enqueue_many(
            "ns:jobs",
            [b"payload"],
            delay=timedelta(seconds=3),
            delayed_delivery=True,
        )

    declare_calls = [call.args[0] for call in mock_channel.declare_queue.await_args_list]
    assert "ns:jobs" in declare_calls
    assert "ns:jobs.__forze_delay" in declare_calls

    publish_queue = mock_channel.default_exchange.publish.await_args.kwargs["routing_key"]
    assert publish_queue == "ns:jobs.__forze_delay"

    message = mock_channel.default_exchange.publish.await_args.args[0]
    assert message.expiration == timedelta(seconds=3)
