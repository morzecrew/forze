"""A default topology (no ``dead_letter_exchange``) destroys every ``nack(requeue=False)``
poison message, and both retention knobs are opt-in — so the consumer entry points
(``receive``/``consume``) warn once per queue when no poison sink is configured, naming the
config keys that enable retention. With a DLX configured the warning never fires."""

from typing import Any

import pytest

pytest.importorskip("aio_pika")

from forze_rabbitmq.kernel.client.client import RabbitMQClient
from forze_rabbitmq.kernel.client.value_objects import RabbitMQConfig

pytestmark = pytest.mark.unit

# ----------------------- #


class _Recorder:
    def __init__(self) -> None:
        self.warnings: list[str] = []

    def warning(self, msg: str, *args: object, **_kw: object) -> None:
        self.warnings.append(msg % args if args else msg)

    def trace(self, *_a: object, **_kw: object) -> None: ...

    def error(self, *_a: object, **_kw: object) -> None: ...


# ....................... #


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


class _ScriptedIterator:
    def __init__(self, script: list[Any]) -> None:
        self._script = list(script)

    async def __aenter__(self) -> "_ScriptedIterator":
        return self

    async def __aexit__(self, *exc: object) -> None: ...

    def __aiter__(self) -> "_ScriptedIterator":
        return self

    async def __anext__(self) -> _FakeIncoming:
        if not self._script:
            raise StopAsyncIteration

        item = self._script.pop(0)

        if item == "timeout":
            raise TimeoutError

        return item


class _FakeQueue:
    def __init__(self, script: list[Any]) -> None:
        self._script = script

    async def bind(self, *_a: object, **_kw: object) -> None: ...

    def iterator(self, **_kwargs: Any) -> _ScriptedIterator:
        return _ScriptedIterator(list(self._script))


class _FakeChannel:
    is_closed = False

    def __init__(self, queue: _FakeQueue) -> None:
        self._queue = queue
        self.declared_exchanges: list[str] = []

    async def declare_queue(
        self,
        name: str,
        durable: bool = True,
        arguments: dict[str, Any] | None = None,
    ) -> _FakeQueue:
        return self._queue

    async def declare_exchange(self, name: str, *_a: object, **_kw: object) -> object:
        self.declared_exchanges.append(name)
        return object()


# ....................... #


def _client(
    script: list[Any],
    monkeypatch: pytest.MonkeyPatch,
    **config_kw: object,
) -> tuple[RabbitMQClient, _Recorder]:
    recorder = _Recorder()
    monkeypatch.setattr(
        "forze_rabbitmq.kernel.client.client.logger",
        recorder,
    )

    client = RabbitMQClient()
    client._RabbitMQClient__config = RabbitMQConfig(**config_kw)  # type: ignore[attr-defined, arg-type]
    client._RabbitMQClient__pending_channel = _FakeChannel(_FakeQueue(script))  # type: ignore[attr-defined]

    return client, recorder


def _poison_warnings(recorder: _Recorder) -> list[str]:
    return [w for w in recorder.warnings if "dead_letter_exchange" in w]


# ----------------------- #


@pytest.mark.asyncio
async def test_receive_without_dlx_warns_once_per_queue(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, recorder = _client([_FakeIncoming("m1")], monkeypatch)

    await client.receive("jobs", limit=1)
    await client.receive("jobs", limit=1)

    warnings = _poison_warnings(recorder)
    assert len(warnings) == 1  # once per queue, not per call
    assert "jobs" in warnings[0]
    # The warning names both retention knobs so an operator can act on it.
    assert "dead_letter_exchange" in warnings[0]
    assert "redelivery_counting" in warnings[0]


@pytest.mark.asyncio
async def test_receive_warns_separately_per_queue(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, recorder = _client([_FakeIncoming("m1")], monkeypatch)

    await client.receive("jobs", limit=1)
    await client.receive("mails", limit=1)

    warnings = _poison_warnings(recorder)
    assert len(warnings) == 2
    assert "jobs" in warnings[0]
    assert "mails" in warnings[1]


@pytest.mark.asyncio
async def test_consume_without_dlx_warns_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, recorder = _client([_FakeIncoming("m1"), "timeout"], monkeypatch)

    async for _ in client.consume("jobs", timeout=None):
        break

    warnings = _poison_warnings(recorder)
    assert len(warnings) == 1
    assert "jobs" in warnings[0]


@pytest.mark.asyncio
async def test_no_warning_when_dlx_configured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, recorder = _client(
        [_FakeIncoming("m1")],
        monkeypatch,
        dead_letter_exchange="dlx",
    )

    await client.receive("jobs", limit=1)

    assert _poison_warnings(recorder) == []


@pytest.mark.asyncio
async def test_warned_set_reset_allows_rewarn(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A teardown clears the warned set (like the DLX-ready guard) so a fresh
    # connection surfaces the misconfiguration again for the new run.
    client, recorder = _client([_FakeIncoming("m1")], monkeypatch)

    await client.receive("jobs", limit=1)
    client._RabbitMQClient__poison_drop_warned_queues.clear()  # type: ignore[attr-defined]
    await client.receive("jobs", limit=1)

    assert len(_poison_warnings(recorder)) == 2
