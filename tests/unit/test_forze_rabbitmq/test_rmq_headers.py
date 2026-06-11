"""Unit tests for RabbitMQ transport headers and delivery_count (no broker)."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest
from pydantic import BaseModel

pytest.importorskip("aio_pika")

from forze.base.serialization import PydanticModelCodec
from forze_rabbitmq.adapters import RabbitMQQueueAdapter, RabbitMQQueueCodec
from forze_rabbitmq.kernel.client import RabbitMQClient, RabbitMQQueueMessage

# ----------------------- #


class _Payload(BaseModel):
    value: str


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


# ----------------------- #
# Publish-side: caller headers ride AMQP headers; reserved keys win.


@pytest.mark.asyncio
async def test_enqueue_headers_ride_amqp_headers_and_reserved_key_wins() -> None:
    client = RabbitMQClient()
    channel = _mock_channel()
    cm = _patched_channel(client, channel)

    try:
        await client.enqueue(
            "ns:jobs",
            b"{}",
            key="real-key",
            headers={"trace": "t-1", "forze_key": "forged"},
        )
    finally:
        cm.stop()

    message = channel.default_exchange.publish.await_args.args[0]

    assert message.headers["trace"] == "t-1"
    # The reserved transport key always wins over a colliding caller header.
    assert message.headers["forze_key"] == "real-key"


@pytest.mark.asyncio
async def test_enqueue_without_headers_keeps_previous_wire_shape() -> None:
    client = RabbitMQClient()
    channel = _mock_channel()
    cm = _patched_channel(client, channel)

    try:
        await client.enqueue("ns:jobs", b"{}")
    finally:
        cm.stop()

    message = channel.default_exchange.publish.await_args.args[0]

    assert not message.headers  # aio_pika normalizes None to an empty mapping


# ----------------------- #
# Receive-side extraction helpers.


class TestExtractHeaders:
    def test_none_headers(self) -> None:
        assert RabbitMQClient._RabbitMQClient__extract_headers(None) is None

    def test_reserved_and_broker_internal_keys_excluded(self) -> None:
        got = RabbitMQClient._RabbitMQClient__extract_headers(
            {
                "forze_key": "k",
                "x-death": [{"count": 1}],
                "trace": "t-1",
                "raw": b"bytes-value",
                "num": 42,
            }
        )

        assert got == {"trace": "t-1", "raw": "bytes-value"}

    def test_empty_result_collapses_to_none(self) -> None:
        assert (
            RabbitMQClient._RabbitMQClient__extract_headers({"forze_key": "k"}) is None
        )


class TestExtractDeliveryCount:
    @staticmethod
    def _raw(*, headers: dict | None, redelivered: bool) -> Mock:
        raw = Mock()
        raw.headers = headers
        raw.redelivered = redelivered

        return raw

    def test_first_delivery(self) -> None:
        raw = self._raw(headers=None, redelivered=False)

        assert RabbitMQClient._RabbitMQClient__extract_delivery_count(raw) == 1

    def test_redelivered_without_history_reports_two(self) -> None:
        raw = self._raw(headers={}, redelivered=True)

        assert RabbitMQClient._RabbitMQClient__extract_delivery_count(raw) == 2

    def test_x_death_rejected_counts_plus_one(self) -> None:
        raw = self._raw(
            headers={
                "x-death": [
                    {"reason": "rejected", "count": 3},
                    {"reason": "expired", "count": 7},  # delay hop: not a delivery
                ]
            },
            redelivered=False,
        )

        assert RabbitMQClient._RabbitMQClient__extract_delivery_count(raw) == 4

    def test_expired_only_x_death_is_first_delivery(self) -> None:
        # A delayed message dead-letters through the delay queue (reason
        # "expired") before its FIRST delivery — that hop must not count.
        raw = self._raw(
            headers={"x-death": [{"reason": "expired", "count": 1}]},
            redelivered=False,
        )

        assert RabbitMQClient._RabbitMQClient__extract_delivery_count(raw) == 1

    def test_bytes_reason_handled(self) -> None:
        raw = self._raw(
            headers={"x-death": [{"reason": b"rejected", "count": 2}]},
            redelivered=True,
        )

        assert RabbitMQClient._RabbitMQClient__extract_delivery_count(raw) == 3


# ----------------------- #
# Adapter + shared codec pass-through.


def test_codec_decodes_headers_and_delivery_count() -> None:
    codec = RabbitMQQueueCodec(payload_codec=PydanticModelCodec(_Payload))
    encoded = codec.encode(_Payload(value="hello"))

    decoded = codec.decode(
        "jobs",
        RabbitMQQueueMessage(
            queue="jobs",
            id="msg-1",
            body=encoded,
            headers={"trace": "t-1"},
            delivery_count=2,
        ),
    )

    assert decoded.headers == {"trace": "t-1"}
    assert decoded.delivery_count == 2


def test_codec_decode_without_headers_defaults_empty() -> None:
    codec = RabbitMQQueueCodec(payload_codec=PydanticModelCodec(_Payload))
    encoded = codec.encode(_Payload(value="hello"))

    decoded = codec.decode(
        "jobs",
        RabbitMQQueueMessage(queue="jobs", id="msg-1", body=encoded),
    )

    assert dict(decoded.headers) == {}
    assert decoded.delivery_count is None


@pytest.mark.asyncio
async def test_adapter_forwards_headers_to_client() -> None:
    client = Mock(spec=RabbitMQClient)
    client.enqueue = AsyncMock(return_value="msg-1")
    client.enqueue_many = AsyncMock(return_value=["msg-1"])
    adapter = RabbitMQQueueAdapter(
        client=client,
        codec=RabbitMQQueueCodec(payload_codec=PydanticModelCodec(_Payload)),
        namespace="ns",
    )

    await adapter.enqueue("jobs", _Payload(value="x"), headers={"trace": "t-1"})
    assert client.enqueue.await_args.kwargs["headers"] == {"trace": "t-1"}

    await adapter.enqueue_many("jobs", [_Payload(value="x")], headers={"a": "b"})
    assert client.enqueue_many.await_args.kwargs["headers"] == {"a": "b"}
