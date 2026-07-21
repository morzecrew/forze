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

    def __init__(
        self, message_id: str, *, fail_nack: bool = False, fail_ack: bool = False
    ) -> None:
        self.message_id = message_id
        self.delivery_tag = 1
        self.fail_nack = fail_nack
        self.fail_ack = fail_ack
        self.nack_calls: list[dict[str, Any]] = []
        self.ack_calls = 0

    async def nack(self, requeue: bool = True) -> None:
        self.nack_calls.append({"requeue": requeue})

        if self.fail_nack:
            raise RuntimeError("channel gone")

    async def ack(self) -> None:
        self.ack_calls += 1

        if self.fail_ack:
            raise RuntimeError("stale delivery tag")


class _FakeChannel:
    is_closed = False

    def __init__(self) -> None:
        self.closed = False
        self.qos: int | None = None

    async def close(self) -> None:
        self.closed = True

    async def set_qos(self, prefetch_count: int) -> None:
        self.qos = prefetch_count


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


class TestAckPartialFailure:
    """One failed ack must not strand the rest of the batch in the pending map."""

    @pytest.mark.asyncio
    async def test_partial_ack_failure_drops_every_attempted_entry(self) -> None:
        good = _FakePendingMessage("good")
        bad = _FakePendingMessage("bad", fail_ack=True)  # stale tag after a blip
        client, _ = _client_with_pending([good, bad])

        # Bare gather would have raised on ``bad`` and left BOTH entries pending; with
        # return_exceptions the good one is acked and every attempted id is dropped.
        acked = await client.ack("q", ["good", "bad"])

        assert acked == 1  # only the good one actually acknowledged
        assert good.ack_calls == 1 and bad.ack_calls == 1
        assert client._RabbitMQClient__pending == {}  # type: ignore[attr-defined]  # no leak


# ....................... #


class _ReopenConnection:
    """A fake robust connection that hands out fresh channels on demand."""

    is_closed = False

    def __init__(self) -> None:
        self.new_channels: list[_FakeChannel] = []

    async def channel(self, publisher_confirms: bool = True) -> _FakeChannel:
        del publisher_confirms
        channel = _FakeChannel()
        self.new_channels.append(channel)
        return channel


class TestReopenPurgesStalePending:
    """A robust-channel reopen after a blip must purge the now-invalid delivery tags."""

    @pytest.mark.asyncio
    async def test_reopen_purges_stale_pending_tags(self) -> None:
        client = RabbitMQClient()
        connection = _ReopenConnection()
        client._RabbitMQClient__connection = connection  # type: ignore[attr-defined]

        # A closed pending channel with entries still mapped to its dead tags.
        closed = _FakeChannel()
        closed.is_closed = True  # type: ignore[attr-defined]
        client._RabbitMQClient__pending_channel = closed  # type: ignore[attr-defined]
        pending = client._RabbitMQClient__pending  # type: ignore[attr-defined]
        for i in range(3):
            pending[f"m{i}"] = ("q", _FakePendingMessage(f"m{i}"))

        channel = await client._RabbitMQClient__require_pending_channel()  # type: ignore[attr-defined]

        assert channel is connection.new_channels[-1]  # a fresh channel was installed
        assert client._RabbitMQClient__pending == {}  # type: ignore[attr-defined]  # stale tags purged

    @pytest.mark.asyncio
    async def test_first_open_does_not_purge(self) -> None:
        # A genuine first-time open (no prior channel) must not touch a map that a
        # concurrent read may have started populating; nothing to purge anyway.
        client = RabbitMQClient()
        client._RabbitMQClient__connection = _ReopenConnection()  # type: ignore[attr-defined]

        await client._RabbitMQClient__require_pending_channel()  # type: ignore[attr-defined]

        # No exception, channel installed; pending untouched (empty here).
        assert client._RabbitMQClient__pending_channel is not None  # type: ignore[attr-defined]


# ....................... #


class TestPendingWatermark:
    def test_watermark_must_be_positive(self) -> None:
        from forze.base.exceptions import CoreException

        with pytest.raises(CoreException):
            RabbitMQConfig(pending_watermark=0)

    def test_redelivery_counting_requires_publisher_confirms(self) -> None:
        # Counted requeue republishes then acks the original — fire-and-forget publishing
        # (no confirms) would ack a message that never reached the broker. Reject the combo.
        from forze.base.exceptions import CoreException

        with pytest.raises(CoreException):
            RabbitMQConfig(redelivery_counting=True, publisher_confirms=False)

        # The safe combinations still construct.
        RabbitMQConfig(redelivery_counting=True)  # publisher_confirms defaults True
        RabbitMQConfig(redelivery_counting=False, publisher_confirms=False)

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
