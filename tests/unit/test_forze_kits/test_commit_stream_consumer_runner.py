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

import pytest
from pydantic import BaseModel

from forze.application.contracts.inbox import InboxSpec
from forze.application.contracts.stream import (
    OffsetReset,
    StreamMessage,
    StreamSpec,
)
from forze.application.execution import ExecutionContext
from forze.base.exceptions import CoreException
from forze.base.serialization import PydanticModelCodec
from tests.support.execution_context import context_from_modules

from forze_kits.integrations.consumer import (
    CommitStreamGroupConsumer,
    CommitStreamGroupConsumerRunResult,
)
from forze_mock import MockDepsModule
from forze_mock.adapters import (
    MockCommitStreamGroupAdapter,
    MockCommitStreamGroupAdminAdapter,
    MockState,
    MockStreamAdapter,
)

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


@pytest.mark.asyncio
async def test_rejects_bad_config() -> None:
    async def handler(_msg: StreamMessage[_Payload]) -> None:
        return None

    with pytest.raises(CoreException):
        _consumer(handler, max_attempts=0)

    with pytest.raises(CoreException):
        _consumer(handler, topics=[])
