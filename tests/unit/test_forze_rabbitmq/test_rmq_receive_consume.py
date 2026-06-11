"""Unit tests for RabbitMQClient receive/consume timeout semantics (no broker).

The aio_pika queue iterator is replaced with a scripted fake mimicking its
contract: per-``__anext__`` idle timeout (raises :class:`TimeoutError` after
closing) and unbounded waits when no timeout is configured.
"""

import asyncio
import time
from datetime import timedelta
from typing import Any

import pytest

pytest.importorskip("aio_pika")

from forze_rabbitmq.kernel.client.client import RabbitMQClient

# ----------------------- #


class _FakeIncoming:
    """Minimal stand-in for aio_pika AbstractIncomingMessage."""

    def __init__(self, message_id: str, body: bytes = b"{}") -> None:
        self.message_id = message_id
        self.delivery_tag = 1
        self.body = body
        self.type = None
        self.timestamp = None
        self.headers = None
        self.redelivered = False


# ....................... #


class _ScriptedIterator:
    """Mimics aio_pika QueueIterator control flow.

    Script items: a message (returned), ``"timeout"`` (raises TimeoutError,
    like an elapsed idle wait), ``"block"`` (waits forever; cancellable).
    """

    def __init__(self, script: list[Any]) -> None:
        self._script = list(script)
        self.closed = False

    async def __aenter__(self) -> "_ScriptedIterator":
        return self

    async def __aexit__(self, *exc: object) -> None:
        self.closed = True

    def __aiter__(self) -> "_ScriptedIterator":
        return self

    async def __anext__(self) -> _FakeIncoming:
        if not self._script:
            raise StopAsyncIteration

        item = self._script.pop(0)

        if item == "timeout":
            raise TimeoutError

        if item == "block":
            await asyncio.Event().wait()

        return item


# ....................... #


class _FakeQueue:
    def __init__(self, script: list[Any]) -> None:
        self._script = script
        self.iterator_kwargs: dict[str, Any] | None = None

    def iterator(self, **kwargs: Any) -> _ScriptedIterator:
        self.iterator_kwargs = kwargs
        return _ScriptedIterator(self._script)


class _FakeChannel:
    is_closed = False

    def __init__(self, queue: _FakeQueue) -> None:
        self._queue = queue

    async def declare_queue(self, name: str, durable: bool = True) -> _FakeQueue:
        return self._queue


# ....................... #


def _client_with_queue(script: list[Any]) -> tuple[RabbitMQClient, _FakeQueue]:
    client = RabbitMQClient()
    queue = _FakeQueue(script)
    client._RabbitMQClient__pending_channel = _FakeChannel(queue)  # type: ignore[attr-defined]

    return client, queue


# ----------------------- #


class TestReceiveBoundedWindow:
    @pytest.mark.asyncio
    async def test_none_timeout_is_bounded_default_window(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """receive(timeout=None) returns [] within the default window."""
        monkeypatch.setattr(
            "forze_rabbitmq.kernel.client.client._DEFAULT_RECEIVE_WINDOW",
            timedelta(seconds=0.1),
        )
        client, _ = _client_with_queue(["block"])

        start = time.monotonic()
        messages = await asyncio.wait_for(client.receive("q"), timeout=2)
        elapsed = time.monotonic() - start

        assert messages == []
        assert elapsed < 1.0

    @pytest.mark.asyncio
    async def test_partial_batch_returned_when_window_elapses(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """receive(limit=5) returns the 1 available message, not a full batch."""
        monkeypatch.setattr(
            "forze_rabbitmq.kernel.client.client._DEFAULT_RECEIVE_WINDOW",
            timedelta(seconds=0.1),
        )
        client, _ = _client_with_queue([_FakeIncoming("m1"), "block"])

        messages = await asyncio.wait_for(
            client.receive("q", limit=5),
            timeout=2,
        )

        assert [m.id for m in messages] == ["m1"]

    @pytest.mark.asyncio
    async def test_returns_early_once_limit_reached(self) -> None:
        """receive does not wait out the window when limit is satisfied."""
        client, _ = _client_with_queue(
            [_FakeIncoming("m1"), _FakeIncoming("m2"), "block"]
        )

        start = time.monotonic()
        messages = await client.receive(
            "q",
            limit=2,
            timeout=timedelta(seconds=10),
        )
        elapsed = time.monotonic() - start

        assert [m.id for m in messages] == ["m1", "m2"]
        assert elapsed < 1.0

    @pytest.mark.asyncio
    async def test_zero_timeout_falls_back_to_default_window(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Explicit timeout=0 must not mean an unbounded block."""
        monkeypatch.setattr(
            "forze_rabbitmq.kernel.client.client._DEFAULT_RECEIVE_WINDOW",
            timedelta(seconds=0.1),
        )
        client, _ = _client_with_queue(["block"])

        messages = await asyncio.wait_for(
            client.receive("q", timeout=timedelta(0)),
            timeout=2,
        )

        assert messages == []


# ....................... #


class TestConsumeIdleTimeout:
    @pytest.mark.asyncio
    async def test_none_timeout_consumes_without_iterator_timeout(self) -> None:
        """timeout=None maps to an unbounded aio_pika wait (consume forever)."""
        client, queue = _client_with_queue([_FakeIncoming("m1"), _FakeIncoming("m2")])
        received = []

        async for msg in client.consume("q"):
            received.append(msg.id)

            if len(received) == 2:
                break

        assert received == ["m1", "m2"]
        assert queue.iterator_kwargs is not None
        assert queue.iterator_kwargs["timeout"] is None

    @pytest.mark.asyncio
    async def test_finite_timeout_terminates_cleanly_on_idle(self) -> None:
        """Idle TimeoutError ends the generator instead of raising."""
        client, queue = _client_with_queue([_FakeIncoming("m1"), "timeout"])
        received = []

        async for msg in client.consume("q", timeout=timedelta(seconds=2)):
            received.append(msg.id)

        assert received == ["m1"]
        assert queue.iterator_kwargs is not None
        assert queue.iterator_kwargs["timeout"] == 2.0

    @pytest.mark.asyncio
    async def test_immediate_idle_timeout_yields_nothing(self) -> None:
        """A consumer idle for the whole window stops without an error."""
        client, _ = _client_with_queue(["timeout"])

        received = [
            msg async for msg in client.consume("q", timeout=timedelta(seconds=1))
        ]

        assert received == []

    @pytest.mark.asyncio
    async def test_non_positive_timeout_means_consume_forever(self) -> None:
        """timeout<=0 falls back to unbounded consumption."""
        client, queue = _client_with_queue([_FakeIncoming("m1")])

        async for _ in client.consume("q", timeout=timedelta(0)):
            break

        assert queue.iterator_kwargs is not None
        assert queue.iterator_kwargs["timeout"] is None
