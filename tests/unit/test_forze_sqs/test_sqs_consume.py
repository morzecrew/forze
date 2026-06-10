"""Unit tests for SQSClient.consume long-polling / idle-timeout semantics (no I/O)."""

import asyncio
from datetime import timedelta
from typing import Any

import pytest

pytest.importorskip("aioboto3")

from forze_sqs.kernel.client import SQSClient

# ----------------------- #

_QUEUE = "jobs"
_QUEUE_URL = "https://sqs.local/1/jobs"


def _message(message_id: str = "r1") -> dict[str, Any]:
    return {
        "MessageId": message_id,
        "ReceiptHandle": f"receipt-{message_id}",
        "Body": "hello",
    }


class _FakeSqs:
    """Scripted receive_message stub recording call kwargs.

    Script items: a response dict (returned), an exception (raised), or
    ``"repeat-empty"`` (returns empty responses forever).
    """

    def __init__(self, script: list[Any]) -> None:
        self._script = list(script)
        self.calls: list[dict[str, Any]] = []

    async def receive_message(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(kwargs)

        item = self._script[0] if self._script else "repeat-empty"

        if item == "repeat-empty":
            await asyncio.sleep(0)
            return {"Messages": []}

        self._script.pop(0)

        if isinstance(item, BaseException):
            raise item

        return item


def _bind(client: SQSClient, fake: _FakeSqs) -> None:
    client._SQSClient__queue_url_cache[_QUEUE] = _QUEUE_URL  # type: ignore[attr-defined]
    client._SQSClient__ctx_client.set(fake)  # type: ignore[attr-defined]
    client._SQSClient__ctx_depth.set(1)  # type: ignore[attr-defined]


# ----------------------- #


class TestConsumeLongPolling:
    @pytest.mark.asyncio
    async def test_none_timeout_uses_max_long_poll_wait(self) -> None:
        """timeout=None long-polls with WaitTimeSeconds=20, no short polling."""
        client = SQSClient()
        fake = _FakeSqs([{"Messages": [_message()]}])
        _bind(client, fake)

        gen = client.consume(_QUEUE)

        try:
            msg = await asyncio.wait_for(gen.__anext__(), timeout=2)
        finally:
            await gen.aclose()

        assert msg.id == "r1"
        assert msg.receipt_handle == "receipt-r1"
        assert msg.body == b"hello"
        assert fake.calls[0]["WaitTimeSeconds"] == 20

    @pytest.mark.asyncio
    async def test_finite_timeout_caps_wait_time(self) -> None:
        """A finite idle timeout bounds WaitTimeSeconds by the remaining idle window."""
        client = SQSClient()
        fake = _FakeSqs([{"Messages": [_message()]}])
        _bind(client, fake)

        gen = client.consume(_QUEUE, timeout=timedelta(seconds=5))

        try:
            await asyncio.wait_for(gen.__anext__(), timeout=2)
        finally:
            await gen.aclose()

        assert fake.calls[0]["WaitTimeSeconds"] == 5


# ....................... #


class TestConsumeIdleTimeout:
    @pytest.mark.asyncio
    async def test_idle_timeout_terminates_cleanly(self) -> None:
        """No messages for the idle window ends the generator without error."""
        client = SQSClient()
        fake = _FakeSqs(["repeat-empty"])
        _bind(client, fake)

        async def _drain() -> list[Any]:
            return [
                msg
                async for msg in client.consume(_QUEUE, timeout=timedelta(seconds=0.2))
            ]

        received = await asyncio.wait_for(_drain(), timeout=3)

        assert received == []
        assert fake.calls  # at least one poll happened

    @pytest.mark.asyncio
    async def test_message_resets_idle_window(self) -> None:
        """A received message extends consumption past the initial deadline."""
        client = SQSClient()
        fake = _FakeSqs(
            [{"Messages": [_message("r1")]}, {"Messages": [_message("r2")]}]
        )
        _bind(client, fake)

        received = []

        async def _drain() -> None:
            async for msg in client.consume(_QUEUE, timeout=timedelta(seconds=0.2)):
                received.append(msg.id)
                # Processing longer than the idle window must not kill the
                # stream: the window restarts when iteration resumes.
                await asyncio.sleep(0.3)

        await asyncio.wait_for(_drain(), timeout=5)

        assert received == ["r1", "r2"]


# ....................... #


class TestConsumeErrorBackoff:
    @pytest.mark.asyncio
    async def test_failed_receive_backs_off_then_recovers(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Errors trigger exponential backoff sleeps instead of a hot loop."""
        client = SQSClient()
        fake = _FakeSqs(
            [
                RuntimeError("boom-1"),
                RuntimeError("boom-2"),
                {"Messages": [_message()]},
            ]
        )
        _bind(client, fake)

        sleeps: list[float] = []
        real_sleep = asyncio.sleep

        async def _fake_sleep(delay: float) -> None:
            sleeps.append(delay)
            await real_sleep(0)

        monkeypatch.setattr(asyncio, "sleep", _fake_sleep)

        gen = client.consume(_QUEUE)

        try:
            msg = await asyncio.wait_for(gen.__anext__(), timeout=2)
        finally:
            await gen.aclose()

        assert msg.id == "r1"
        assert sleeps == [0.5, 1.0]
        assert len(fake.calls) == 3

    @pytest.mark.asyncio
    async def test_failing_receive_still_honors_idle_timeout(self) -> None:
        """A persistently failing receive terminates at the idle deadline."""
        client = SQSClient()
        fake = _FakeSqs([RuntimeError("down")] * 50)
        _bind(client, fake)

        async def _drain() -> list[Any]:
            return [
                msg
                async for msg in client.consume(_QUEUE, timeout=timedelta(seconds=0.3))
            ]

        received = await asyncio.wait_for(_drain(), timeout=5)

        assert received == []
        assert len(fake.calls) < 10  # backoff prevented a hot loop
