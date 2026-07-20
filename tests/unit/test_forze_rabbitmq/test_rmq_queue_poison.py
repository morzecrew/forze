"""Unit tests for the queue adapter's poison-message handling (no broker).

A codec decode failure must nack that message with ``requeue=False`` (DLX
per the nack contract), log an error with the message id (never the
payload), and keep the receive batch / consume loop alive.
"""

from datetime import timedelta
from typing import Any
from unittest.mock import AsyncMock, Mock

import pytest
from pydantic import BaseModel

pytest.importorskip("aio_pika")

from forze.base.serialization import PydanticModelCodec
from forze_rabbitmq.adapters import RabbitMQQueueAdapter, RabbitMQQueueCodec
from forze_rabbitmq.kernel.client import RabbitMQClient, RabbitMQQueueMessage

# ----------------------- #

_GARBAGE = b"\x00not-json-at-all"


class _Payload(BaseModel):
    value: str


class _LoggerStub:
    def __init__(self) -> None:
        self.errors: list[tuple[Any, ...]] = []

    def error(self, event: str, *sub: Any, **extras: Any) -> None:
        self.errors.append((event, *sub))


@pytest.fixture()
def logger_stub(monkeypatch: pytest.MonkeyPatch) -> _LoggerStub:
    stub = _LoggerStub()
    monkeypatch.setattr("forze_rabbitmq.adapters.queue.logger", stub)

    return stub


def _codec() -> RabbitMQQueueCodec[_Payload]:
    return RabbitMQQueueCodec(payload_codec=PydanticModelCodec(_Payload))


def _raw(codec: RabbitMQQueueCodec[_Payload], message_id: str, value: str | None):
    """Raw message; ``value=None`` produces an undecodable (poison) body."""
    body = _GARBAGE if value is None else codec.encode(_Payload(value=value))

    return RabbitMQQueueMessage(queue="ns:jobs", id=message_id, body=body)


# ----------------------- #


class TestReceivePoison:
    @pytest.mark.asyncio
    async def test_poison_entry_nacked_and_remainder_returned(
        self, logger_stub: _LoggerStub
    ) -> None:
        codec = _codec()
        client = Mock(spec=RabbitMQClient)
        client.receive = AsyncMock(
            return_value=[
                _raw(codec, "poison-1", None),
                _raw(codec, "good-1", "hello"),
                _raw(codec, "poison-2", None),
            ]
        )
        client.nack = AsyncMock(return_value=1)
        adapter = RabbitMQQueueAdapter(client=client, codec=codec, namespace="ns")

        messages = await adapter.receive("jobs", limit=3)

        assert [m.id for m in messages] == ["good-1"]
        assert messages[0].payload.value == "hello"

        nacked = [call.args + (call.kwargs,) for call in client.nack.await_args_list]
        assert nacked == [
            ("ns:jobs", ["poison-1"], {"requeue": False}),
            ("ns:jobs", ["poison-2"], {"requeue": False}),
        ]

    @pytest.mark.asyncio
    async def test_poison_logged_with_id_never_payload(
        self, logger_stub: _LoggerStub
    ) -> None:
        codec = _codec()
        client = Mock(spec=RabbitMQClient)
        client.receive = AsyncMock(return_value=[_raw(codec, "poison-1", None)])
        client.nack = AsyncMock(return_value=1)
        adapter = RabbitMQQueueAdapter(client=client, codec=codec, namespace="ns")

        assert await adapter.receive("jobs") == []

        assert len(logger_stub.errors) == 1
        logged = logger_stub.errors[0]
        assert "poison-1" in logged
        assert all(_GARBAGE not in str(part).encode() for part in logged)

    @pytest.mark.asyncio
    async def test_all_poison_batch_returns_empty(
        self, logger_stub: _LoggerStub
    ) -> None:
        codec = _codec()
        client = Mock(spec=RabbitMQClient)
        client.receive = AsyncMock(
            return_value=[_raw(codec, "p1", None), _raw(codec, "p2", None)]
        )
        client.nack = AsyncMock(return_value=1)
        adapter = RabbitMQQueueAdapter(client=client, codec=codec, namespace="ns")

        assert await adapter.receive("jobs", limit=2) == []
        assert client.nack.await_count == 2


# ....................... #


class TestConsumePoison:
    @staticmethod
    def _consuming_client(
        codec: RabbitMQQueueCodec[_Payload],
        raws: list[RabbitMQQueueMessage],
    ) -> Mock:
        client = Mock(spec=RabbitMQClient)

        async def _iter():
            for raw in raws:
                yield raw

        def _consume(queue: str, timeout: timedelta | None = None):
            return _iter()

        client.consume = Mock(side_effect=_consume)
        client.nack = AsyncMock(return_value=1)

        return client

    # ....................... #

    @pytest.mark.asyncio
    async def test_poison_nacked_and_loop_continues(
        self, logger_stub: _LoggerStub
    ) -> None:
        codec = _codec()
        client = self._consuming_client(
            codec,
            [
                _raw(codec, "poison-1", None),
                _raw(codec, "good-1", "hello"),
                _raw(codec, "good-2", "world"),
            ],
        )
        adapter = RabbitMQQueueAdapter(client=client, codec=codec, namespace="ns")

        received = [msg.id async for msg in adapter.consume("jobs")]

        assert received == ["good-1", "good-2"]
        client.nack.assert_awaited_once_with("ns:jobs", ["poison-1"], requeue=False)
        assert len(logger_stub.errors) == 1
        assert "poison-1" in logger_stub.errors[0]

    @pytest.mark.asyncio
    async def test_nack_failure_does_not_crash_the_loop(
        self, logger_stub: _LoggerStub
    ) -> None:
        codec = _codec()
        client = self._consuming_client(
            codec,
            [_raw(codec, "poison-1", None), _raw(codec, "good-1", "hello")],
        )
        client.nack = AsyncMock(side_effect=RuntimeError("broker hiccup"))
        adapter = RabbitMQQueueAdapter(client=client, codec=codec, namespace="ns")

        received = [msg.id async for msg in adapter.consume("jobs")]

        assert received == ["good-1"]
        # decode error + nack failure
        assert len(logger_stub.errors) == 2
