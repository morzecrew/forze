"""Offset-log consumer runner: the commit-after-inbox decision ladder.

The mock offset-log is the primary backend: a per-group committed cursor makes
processed / duplicate / dead-lettered / paused outcomes observable, and the same
in-memory state feeds a producer handle for seeding the log.

``strict_tx=True`` everywhere: a failing handler must roll the inbox mark back
with its transaction, or retry / poison semantics could not be exercised.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import Any

import attrs
import pytest
from pydantic import BaseModel

from forze.application.contracts.inbox import InboxDepKey, InboxSpec
from forze.application.contracts.resilience import ResilienceExecutorDepKey
from forze.application.contracts.stream import (
    CommitStreamGroupQueryDepKey,
    OffsetReset,
    StreamMessage,
    StreamPosition,
    StreamSpec,
    UndecodableStreamPayload,
)
from forze.application.contracts.transaction import TransactionManagerDepKey
from forze.application.execution import Deps, ExecutionContext
from forze.base.exceptions import CoreException
from forze.base.primitives import StrKey
from forze.base.serialization import PydanticModelCodec
from tests.support.execution_context import context_from_deps, context_from_modules

from forze_kits.integrations.consumer import (
    CommitStreamGroupConsumer,
    CommitStreamGroupConsumerRunResult,
)
from forze_mock import MockDepsModule, MockStateDepKey
from forze_mock.adapters import (
    MockCommitStreamGroupAdapter,
    MockCommitStreamGroupAdminAdapter,
    MockState,
    MockStreamAdapter,
)
from forze_mock.execution.module import ConfigurableMockInbox, mock_strict_txmanager

# ----------------------- #


class _Payload(BaseModel):
    value: str


_CODEC = PydanticModelCodec(_Payload)
_TOPIC = "orders"
_STREAM_SPEC = StreamSpec(name=_TOPIC, codec=_CODEC)
_INBOX_SPEC = InboxSpec(name="events")
_IDLE = timedelta(milliseconds=100)


def _harness() -> tuple[
    ExecutionContext,
    MockStreamAdapter[_Payload],
    MockCommitStreamGroupAdminAdapter[_Payload],
    MockCommitStreamGroupAdapter[_Payload],
    MockState,
]:
    state = MockState()
    ctx = context_from_modules(MockDepsModule(state=state, strict_tx=True))
    producer = MockStreamAdapter(state=state, namespace=_TOPIC, codec=_CODEC)
    admin = MockCommitStreamGroupAdminAdapter(stream=producer, state=state)
    query = MockCommitStreamGroupAdapter(stream=producer, state=state, namespace=_TOPIC)
    return ctx, producer, admin, query, state


async def _seed(producer: MockStreamAdapter[_Payload], n: int) -> None:
    for i in range(n):
        await producer.append(_TOPIC, _Payload(value=str(i)), key="k")


def _consumer(
    handler: Callable[[StreamMessage[_Payload]], Awaitable[None]],
    **overrides: Any,
) -> CommitStreamGroupConsumer[_Payload]:
    kwargs: dict[str, Any] = dict(
        topics=[_TOPIC],
        group="g",
        consumer="c",
        stream_spec=_STREAM_SPEC,
        handler=handler,
        inbox_spec=_INBOX_SPEC,
        tx_route="default",
    )
    kwargs.update(overrides)
    return CommitStreamGroupConsumer(**kwargs)


# ....................... #


@pytest.mark.asyncio
async def test_processes_and_commits_all() -> None:
    ctx, producer, admin, query, _state = _harness()
    await admin.ensure_group("g", [_TOPIC], start=OffsetReset.EARLIEST)
    await _seed(producer, 3)

    seen: list[str] = []

    async def handler(msg: StreamMessage[_Payload]) -> None:
        seen.append(msg.payload.value)

    result = await _consumer(handler).run(ctx, timeout=_IDLE)

    assert isinstance(result, CommitStreamGroupConsumerRunResult)
    assert result.processed == 3
    assert seen == ["0", "1", "2"]
    # Committed past everything → a fresh read is empty.
    assert await query.read("g", "c", [_TOPIC]) == []


@pytest.mark.asyncio
async def test_redelivery_is_deduped_by_inbox() -> None:
    ctx, producer, admin, query, _state = _harness()
    await admin.ensure_group("g", [_TOPIC], start=OffsetReset.EARLIEST)
    await _seed(producer, 2)

    calls = 0

    async def handler(_msg: StreamMessage[_Payload]) -> None:
        nonlocal calls
        calls += 1

    first = await _consumer(handler).run(ctx, timeout=_IDLE)
    assert first.processed == 2

    # Replay the same offsets; the inbox recognizes them as duplicates.
    await admin.reset_offsets("g", _TOPIC, to=OffsetReset.EARLIEST)
    second = await _consumer(handler).run(ctx, timeout=_IDLE)

    assert second.duplicates == 2
    assert second.processed == 0
    assert calls == 2  # handler ran only for the genuinely-new messages


@pytest.mark.asyncio
async def test_poison_dead_letters_and_advances() -> None:
    ctx, producer, admin, query, state = _harness()
    await admin.ensure_group("g", [_TOPIC], start=OffsetReset.EARLIEST)
    await _seed(producer, 1)

    async def handler(_msg: StreamMessage[_Payload]) -> None:
        raise ValueError("boom")

    result = await _consumer(handler, dlq_stream="orders.dlq").run(ctx, timeout=_IDLE)

    assert result.dead_lettered == 1
    assert result.failed == 0
    # Advanced past the poison message.
    assert await query.read("g", "c", [_TOPIC]) == []
    # Produced to the dead-letter stream.
    assert len(state.streams[_TOPIC]["orders.dlq"]) == 1


@pytest.mark.asyncio
async def test_poison_without_dlq_pauses_and_alerts() -> None:
    ctx, producer, admin, query, _state = _harness()
    await admin.ensure_group("g", [_TOPIC], start=OffsetReset.EARLIEST)
    await _seed(producer, 2)

    async def handler(msg: StreamMessage[_Payload]) -> None:
        if msg.payload.value == "0":
            raise ValueError("boom")

    result = await _consumer(handler).run(ctx, timeout=_IDLE)

    assert result.failed == 1
    assert result.processed == 0
    # Offset left uncommitted → the poison message is redelivered on the next read.
    remaining = await query.read("g", "c", [_TOPIC])
    assert [m.offset for m in remaining] == [0, 1]


@pytest.mark.asyncio
async def test_max_attempts_retries_then_succeeds() -> None:
    ctx, producer, admin, _query, _state = _harness()
    await admin.ensure_group("g", [_TOPIC], start=OffsetReset.EARLIEST)
    await _seed(producer, 1)

    attempts = 0

    async def handler(_msg: StreamMessage[_Payload]) -> None:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise ValueError("transient")

    result = await _consumer(handler, max_attempts=3).run(ctx, timeout=_IDLE)

    assert result.processed == 1
    assert attempts == 3


@pytest.mark.asyncio
async def test_requires_transactions_fails_closed() -> None:
    ctx, producer, admin, _query, _state = _harness()
    await admin.ensure_group("g", [_TOPIC], start=OffsetReset.EARLIEST)
    await _seed(producer, 1)

    spec = StreamSpec(name=_TOPIC, codec=_CODEC, requires_transactions=True)

    async def handler(_msg: StreamMessage[_Payload]) -> None:
        return None

    with pytest.raises(CoreException) as ei:
        await _consumer(handler, stream_spec=spec).run(ctx, timeout=_IDLE)

    assert ei.value.code == "stream.transactions_unsupported"


@attrs.define(slots=True)
class _RetryOnceExecutor:
    """Resilience executor double: records the policy and retries the call once."""

    policies_used: list[str] = attrs.field(factory=list)

    async def run[T](
        self,
        fn: Callable[[], Awaitable[T]],
        *,
        policy: StrKey,
        route: StrKey | None = None,
        fallback: Callable[[BaseException], Awaitable[T]] | None = None,
    ) -> T:
        del route, fallback
        self.policies_used.append(str(policy))
        try:
            return await fn()
        except Exception:  # noqa: BLE001 — retry-once double
            return await fn()


@pytest.mark.asyncio
async def test_retry_policy_wraps_each_process_attempt() -> None:
    state = MockState()
    producer = MockStreamAdapter(state=state, namespace=_TOPIC, codec=_CODEC)
    admin = MockCommitStreamGroupAdminAdapter(stream=producer, state=state)
    query = MockCommitStreamGroupAdapter(stream=producer, state=state, namespace=_TOPIC)

    executor = _RetryOnceExecutor()
    ctx = context_from_deps(
        Deps.plain(
            {
                MockStateDepKey: state,
                InboxDepKey: ConfigurableMockInbox(module=MockDepsModule(state=state)),
                TransactionManagerDepKey: mock_strict_txmanager,
                CommitStreamGroupQueryDepKey: (lambda _ctx, _spec: query),
                ResilienceExecutorDepKey: executor,
            }
        )
    )

    await admin.ensure_group("g", [_TOPIC], start=OffsetReset.EARLIEST)
    await _seed(producer, 1)

    attempts = 0

    async def handler(_msg: StreamMessage[_Payload]) -> None:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise ValueError("transient")

    result = await _consumer(handler, retry_policy="flaky").run(ctx, timeout=_IDLE)

    assert result.processed == 1
    assert attempts == 2  # the executor retried the failing attempt once
    assert executor.policies_used == ["flaky"]


@attrs.define(slots=True)
class _PoisonThenEmptyPort:
    """Fake commit-stream port: serves one undecodable-marker batch, then drains.

    Records commits and seek-to-committed calls so the runner's decode-poison
    pause path (surface marker → pause → rewind to committed) is observable
    without a real Kafka adapter.
    """

    seek_calls: list[tuple[str, list[str]]] = attrs.field(factory=list)
    commits: list[list[StreamPosition]] = attrs.field(factory=list)
    served: bool = False

    async def read(
        self,
        group: str,
        consumer: str,
        topics: Any,
        *,
        limit: Any = None,
        timeout: Any = None,
    ) -> list[StreamMessage[_Payload]]:
        del group, consumer, topics, limit, timeout
        if self.served:
            return []
        self.served = True
        return [
            StreamMessage(
                stream=_TOPIC,
                id=f"{_TOPIC}:0:0",
                payload=UndecodableStreamPayload(raw=b"x", error="boom"),  # type: ignore[arg-type]
                partition=0,
                offset=0,
            )
        ]

    async def tail(self, *args: Any, **kwargs: Any) -> Any:  # pragma: no cover
        raise NotImplementedError

    async def commit(self, group: str, positions: Any) -> None:
        del group
        self.commits.append(list(positions))

    async def seek_to_committed(self, group: str, topics: Any) -> None:
        self.seek_calls.append((group, list(topics)))


@pytest.mark.asyncio
async def test_undecodable_marker_pauses_and_seeks_to_committed() -> None:
    # BUG 1: an UndecodableStreamPayload surfaced by the read path pauses the run
    # (offset left uncommitted) and rewinds to committed, never committing past it.
    port = _PoisonThenEmptyPort()
    ctx = context_from_deps(
        Deps.plain({CommitStreamGroupQueryDepKey: (lambda _ctx, _spec: port)})
    )

    async def handler(_msg: StreamMessage[_Payload]) -> None:  # pragma: no cover
        raise AssertionError("handler must not run on a decode-poison marker")

    result = await _consumer(handler).run(ctx, timeout=_IDLE)

    assert (result.failed, result.processed) == (1, 0)
    assert port.commits == []  # nothing good before the poison → no offset committed
    assert port.seek_calls == [("g", [_TOPIC])]


@pytest.mark.asyncio
async def test_rejects_bad_config() -> None:
    async def handler(_msg: StreamMessage[_Payload]) -> None:
        return None

    with pytest.raises(CoreException):
        _consumer(handler, max_attempts=0)

    with pytest.raises(CoreException):
        _consumer(handler, topics=[])
