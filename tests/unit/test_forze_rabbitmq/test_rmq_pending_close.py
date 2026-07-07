"""Unit tests for the RabbitMQ pending-delivery map lifecycle (no broker).

Covers the leak fixes: ``close()`` nacks (requeue) every pending delivery
best-effort before clearing the map, and crossing the pending watermark logs
a single warning instead of growing silently.
"""

from typing import Any

import pytest

pytest.importorskip("aio_pika")

from forze_rabbitmq.kernel.client.client import RabbitMQClient
from forze_rabbitmq.kernel.client.value_objects import RabbitMQConfig

# ----------------------- #


class _FakePendingMessage:
    """Stand-in for an unacked aio_pika incoming message."""

    def __init__(self, message_id: str, *, fail_nack: bool = False) -> None:
        self.message_id = message_id
        self.delivery_tag = 1
        self.fail_nack = fail_nack
        self.nack_calls: list[dict[str, Any]] = []

    async def nack(self, requeue: bool = True) -> None:
        self.nack_calls.append({"requeue": requeue})

        if self.fail_nack:
            raise RuntimeError("channel gone")


class _FakeChannel:
    is_closed = False

    def __init__(self) -> None:
        self.closed = False

    async def close(self) -> None:
        self.closed = True


class _LoggerStub:
    def __init__(self) -> None:
        self.warnings: list[tuple[Any, ...]] = []
        self.errors: list[tuple[Any, ...]] = []

    def warning(self, event: str, *sub: Any, **extras: Any) -> None:
        self.warnings.append((event, *sub))

    def error(self, event: str, *sub: Any, **extras: Any) -> None:
        self.errors.append((event, *sub))

    def trace(self, event: str, *sub: Any, **extras: Any) -> None:
        pass

    def debug(self, event: str, *sub: Any, **extras: Any) -> None:
        pass


# ....................... #


def _client_with_pending(
    messages: list[_FakePendingMessage],
) -> tuple[RabbitMQClient, _FakeChannel]:
    client = RabbitMQClient()
    channel = _FakeChannel()
    client._RabbitMQClient__pending_channel = channel  # type: ignore[attr-defined]
    pending = client._RabbitMQClient__pending  # type: ignore[attr-defined]

    for message in messages:
        pending[message.message_id] = ("q", message)

    return client, channel


# ----------------------- #


class TestCloseNacksPending:
    @pytest.mark.asyncio
    async def test_close_nacks_all_pending_with_requeue(self) -> None:
        messages = [_FakePendingMessage(f"m{i}") for i in range(3)]
        client, channel = _client_with_pending(messages)

        await client.close()

        for message in messages:
            assert message.nack_calls == [{"requeue": True}]

        assert client._RabbitMQClient__pending == {}  # type: ignore[attr-defined]
        assert channel.closed is True

    @pytest.mark.asyncio
    async def test_close_survives_nack_failures(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A failing nack is logged, never raised, and never blocks others."""
        stub = _LoggerStub()
        monkeypatch.setattr(
            "forze_rabbitmq.kernel.client.client.logger",
            stub,
        )

        good = _FakePendingMessage("good")
        bad = _FakePendingMessage("bad", fail_nack=True)
        client, _ = _client_with_pending([bad, good])

        await client.close()

        assert good.nack_calls == [{"requeue": True}]
        assert bad.nack_calls == [{"requeue": True}]
        assert client._RabbitMQClient__pending == {}  # type: ignore[attr-defined]

        logged = [entry for entry in stub.warnings if "bad" in entry]
        assert len(logged) == 1

    @pytest.mark.asyncio
    async def test_close_without_pending_is_noop(self) -> None:
        client, channel = _client_with_pending([])

        await client.close()

        assert channel.closed is True


# ....................... #


class TestPendingWatermark:
    def test_watermark_must_be_positive(self) -> None:
        from forze.base.exceptions import CoreException

        with pytest.raises(CoreException):
            RabbitMQConfig(pending_watermark=0)

    # ....................... #

    @staticmethod
    def _client_with_watermark(watermark: int) -> RabbitMQClient:
        client = RabbitMQClient()
        client._RabbitMQClient__config = RabbitMQConfig(  # type: ignore[attr-defined]
            pending_watermark=watermark
        )

        return client

    # ....................... #

    @pytest.mark.asyncio
    async def test_warning_logged_once_when_watermark_crossed(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        stub = _LoggerStub()
        monkeypatch.setattr("forze_rabbitmq.kernel.client.client.logger", stub)

        client = self._client_with_watermark(3)
        register = client._RabbitMQClient__register_pending_batch  # type: ignore[attr-defined]

        await register("q", [_FakePendingMessage(f"m{i}") for i in range(4)])
        await register("q", [_FakePendingMessage(f"n{i}") for i in range(4)])

        assert len(stub.warnings) == 1

    @pytest.mark.asyncio
    async def test_warning_rearms_after_map_drains(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        stub = _LoggerStub()
        monkeypatch.setattr("forze_rabbitmq.kernel.client.client.logger", stub)

        client = self._client_with_watermark(4)
        register = client._RabbitMQClient__register_pending_batch  # type: ignore[attr-defined]
        drop = client._RabbitMQClient__drop_pending_many  # type: ignore[attr-defined]

        ids = await register("q", [_FakePendingMessage(f"m{i}") for i in range(5)])
        assert len(stub.warnings) == 1

        await drop(ids)  # drains to zero -> re-arms

        await register("q", [_FakePendingMessage(f"n{i}") for i in range(5)])
        assert len(stub.warnings) == 2

    @pytest.mark.asyncio
    async def test_no_warning_below_watermark(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        stub = _LoggerStub()
        monkeypatch.setattr("forze_rabbitmq.kernel.client.client.logger", stub)

        client = self._client_with_watermark(10)
        register = client._RabbitMQClient__register_pending_batch  # type: ignore[attr-defined]

        await register("q", [_FakePendingMessage(f"m{i}") for i in range(10)])

        assert stub.warnings == []


# ....................... #


class TestCloseCountedRequeue:
    """With ``redelivery_counting`` on, close-time requeues go through the counted republish
    path (per queue) so a poison message left pending at shutdown keeps advancing its count."""

    @pytest.mark.asyncio
    async def test_close_routes_pending_through_counted_requeue(self) -> None:
        from unittest.mock import AsyncMock

        client = RabbitMQClient()
        client._RabbitMQClient__config = RabbitMQConfig(  # type: ignore[attr-defined]
            redelivery_counting=True
        )
        channel = _FakeChannel()
        client._RabbitMQClient__pending_channel = channel  # type: ignore[attr-defined]

        m_a1, m_a2, m_b = (
            _FakePendingMessage("a1"),
            _FakePendingMessage("a2"),
            _FakePendingMessage("b1"),
        )
        pending = client._RabbitMQClient__pending  # type: ignore[attr-defined]
        pending["a1"] = ("qa", m_a1)
        pending["a2"] = ("qa", m_a2)
        pending["b1"] = ("qb", m_b)

        counted = AsyncMock()
        client._RabbitMQClient__requeue_counted = counted  # type: ignore[attr-defined]

        await client.close()

        # Grouped per queue; the plain broker nack path is never taken.
        by_queue = {
            call.args[0]: [m.message_id for m in call.args[1]]
            for call in counted.await_args_list
        }
        assert by_queue == {"qa": ["a1", "a2"], "qb": ["b1"]}
        assert m_a1.nack_calls == [] and m_a2.nack_calls == [] and m_b.nack_calls == []
        assert client._RabbitMQClient__pending == {}  # type: ignore[attr-defined]

    @pytest.mark.asyncio
    async def test_counted_requeue_failure_is_swallowed(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from unittest.mock import AsyncMock

        stub = _LoggerStub()
        monkeypatch.setattr("forze_rabbitmq.kernel.client.client.logger", stub)

        client = RabbitMQClient()
        client._RabbitMQClient__config = RabbitMQConfig(  # type: ignore[attr-defined]
            redelivery_counting=True
        )
        client._RabbitMQClient__pending_channel = _FakeChannel()  # type: ignore[attr-defined]
        client._RabbitMQClient__pending["x"] = ("qx", _FakePendingMessage("x"))  # type: ignore[attr-defined]

        client._RabbitMQClient__requeue_counted = AsyncMock(  # type: ignore[attr-defined]
            side_effect=RuntimeError("channel gone")
        )

        await client.close()  # must not raise

        assert any("qx" in entry for entry in stub.warnings)
        assert client._RabbitMQClient__pending == {}  # type: ignore[attr-defined]
