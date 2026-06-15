"""Queue-consumer runner: every branch of the per-message decision ladder.

The mock queue is the primary backend: visibility-timeout redelivery, exact
``delivery_count``, and an inspectable ``dead_letters`` list make every ladder
outcome observable. Scripted port stubs cover the branches the mock cannot
fake (``delivery_count=None``, ack/nack failures).

``strict_tx=True`` everywhere: a failing handler must roll the inbox mark
back with its transaction, or retry semantics could not be exercised.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, Awaitable, Callable
from datetime import timedelta
from typing import Any

import attrs
import pytest
from pydantic import BaseModel

from forze.application.contracts.inbox import InboxDepKey, InboxSpec
from forze.application.contracts.queue import (
    QueueMessage,
    QueueQueryDepKey,
    QueueSpec,
)
from forze.application.contracts.resilience import ResilienceExecutorDepKey
from forze.application.contracts.transaction import TransactionManagerDepKey
from forze.application.execution import Deps, ExecutionContext
from forze.base.exceptions import CoreException
from forze.base.primitives import StrKey
from forze.base.serialization import PydanticModelCodec
from tests.support.execution_context import context_from_deps, context_from_modules

from forze_kits.integrations.consumer import ConsumerRunResult, QueueConsumer
from forze_mock import MockDepsModule, MockStateDepKey
from forze_mock.adapters import MockQueueAdapter, MockState
from forze_mock.execution.module import (
    ConfigurableMockInbox,
    ConfigurableMockQueue,
    mock_strict_txmanager,
)

# ----------------------- #


class _Payload(BaseModel):
    value: str


_CODEC = PydanticModelCodec(_Payload)
_QUEUE_SPEC = QueueSpec(name="jobs", codec=_CODEC)
_INBOX_SPEC = InboxSpec(name="events")

_IDLE = timedelta(milliseconds=250)
"""Finite idle timeout: ends the run once the queue stays quiet."""


# ....................... #


def _mock_harness() -> tuple[ExecutionContext, MockQueueAdapter[_Payload], MockState]:
    """Context on the full mock module + a same-state adapter handle for the test."""

    state = MockState()
    ctx = context_from_modules(MockDepsModule(state=state, strict_tx=True))
    # Same namespace ConfigurableMockQueue derives from the spec name.
    adapter = MockQueueAdapter[_Payload](state=state, namespace="jobs", codec=_CODEC)

    return ctx, adapter, state


# ....................... #


def _plain_ctx(
    *,
    state: MockState,
    queue_port: Any | None = None,
    resilience: Any | None = None,
) -> ExecutionContext:
    """Minimal context: strict tx + mock inbox + a swappable queue query port."""

    module = MockDepsModule(state=state, strict_tx=True)
    deps: dict[Any, Any] = {
        MockStateDepKey: state,
        InboxDepKey: ConfigurableMockInbox(module=module),
        TransactionManagerDepKey: mock_strict_txmanager,
        QueueQueryDepKey: (
            (lambda _ctx, _spec: queue_port)
            if queue_port is not None
            else ConfigurableMockQueue(module=module)
        ),
    }

    if resilience is not None:
        deps[ResilienceExecutorDepKey] = resilience

    return context_from_deps(Deps.plain(deps))


# ....................... #


def _pending(state: MockState) -> dict[str, Any]:
    """In-flight (received, unacked) entries for the test queue."""

    return state.queue_pending.get("jobs", {}).get("jobs", {})


# ....................... #


async def _run(
    ctx: ExecutionContext,
    handler: Callable[[QueueMessage[_Payload]], Awaitable[None]],
    **overrides: Any,
) -> ConsumerRunResult:
    timeout = overrides.pop("timeout", _IDLE)
    config: dict[str, Any] = {
        "queue": "jobs",
        "queue_spec": _QUEUE_SPEC,
        "handler": handler,
        "inbox_spec": _INBOX_SPEC,
        "tx_route": "mock",
    }
    config.update(overrides)

    return await QueueConsumer(**config).run(ctx, timeout=timeout)


# ----------------------- #
# Stubs for branches the mock queue cannot fake


@attrs.define(slots=True, kw_only=True)
class _ScriptedQueue:
    """Query-port stub yielding a fixed script; records dispositions.

    ``fail_acks_for`` / ``fail_nacks_for`` make the *first* disposition call
    for those ids raise (ack/nack-failure resilience branches).
    """

    script: list[QueueMessage[Any]]
    acked: list[str] = attrs.field(factory=list)
    nacked: list[tuple[str, bool]] = attrs.field(factory=list)
    fail_acks_for: set[str] = attrs.field(factory=set)
    fail_nacks_for: set[str] = attrs.field(factory=set)

    async def consume(
        self,
        queue: str,
        *,
        timeout: timedelta | None = None,
    ) -> AsyncGenerator[QueueMessage[Any]]:
        del queue, timeout

        for message in self.script:
            yield message

    async def receive(
        self,
        queue: str,
        *,
        limit: int | None = None,
        timeout: timedelta | None = None,
    ) -> list[QueueMessage[Any]]:
        del queue, limit, timeout
        return []

    async def ack(self, queue: str, ids: Any) -> int:
        del queue

        for item_id in ids:
            if item_id in self.fail_acks_for:
                self.fail_acks_for.discard(item_id)
                raise RuntimeError("ack exploded")

        self.acked.extend(ids)
        return len(ids)

    async def nack(self, queue: str, ids: Any, *, requeue: bool = True) -> int:
        del queue

        for item_id in ids:
            if item_id in self.fail_nacks_for:
                self.fail_nacks_for.discard(item_id)
                raise RuntimeError("nack exploded")

        self.nacked.extend((item_id, requeue) for item_id in ids)
        return len(ids)


# ....................... #


def _message(
    message_id: str,
    key: str,
    *,
    delivery_count: int | None = None,
) -> QueueMessage[_Payload]:
    return QueueMessage(
        queue="jobs",
        id=message_id,
        payload=_Payload(value=key),
        key=key,
        delivery_count=delivery_count,
    )


# ....................... #


@attrs.define(slots=True)
class _CountingResilienceExecutor:
    """Executor double: records the policy/route and retries the call once."""

    policies_used: list[str] = attrs.field(factory=list)
    routes_used: list[str | None] = attrs.field(factory=list)

    async def run[T](
        self,
        fn: Callable[[], Awaitable[T]],
        *,
        policy: StrKey,
        route: StrKey | None = None,
        fallback: Callable[[BaseException], Awaitable[T]] | None = None,
    ) -> T:
        del fallback
        self.policies_used.append(str(policy))
        self.routes_used.append(None if route is None else str(route))

        try:
            return await fn()

        except Exception:  # noqa: BLE001 — retry-once double
            return await fn()

    async def run_hedged[T](
        self,
        fn: Callable[[], Awaitable[T]],
        *,
        policy: StrKey,
        route: StrKey | None = None,
    ) -> T:
        del policy, route
        return await fn()


# ----------------------- #
# Happy path + duplicates


async def test_happy_path_processes_and_acks() -> None:
    ctx, q, state = _mock_harness()
    seen: list[str] = []

    async def handler(msg: QueueMessage[_Payload]) -> None:
        seen.append(msg.payload.value)

    await q.enqueue("jobs", _Payload(value="a"), key="evt-a")
    await q.enqueue("jobs", _Payload(value="b"), key="evt-b")

    result = await _run(ctx, handler)

    assert result == ConsumerRunResult(processed=2)
    assert sorted(seen) == ["a", "b"]
    # Acked: nothing in-flight, nothing receivable, nothing dead-lettered.
    assert _pending(state) == {}
    assert await q.receive("jobs") == []
    assert q.dead_letters("jobs") == []


# ....................... #


async def test_duplicate_redelivery_acked_without_handler_rerun() -> None:
    # At-least-once relay: the same event key published as two broker messages.
    ctx, q, state = _mock_harness()
    calls: list[str] = []

    async def handler(msg: QueueMessage[_Payload]) -> None:
        calls.append(msg.id)

    await q.enqueue("jobs", _Payload(value="x"), key="evt-1")
    first = await _run(ctx, handler)

    await q.enqueue("jobs", _Payload(value="x"), key="evt-1")
    second = await _run(ctx, handler)

    assert first == ConsumerRunResult(processed=1)
    assert second == ConsumerRunResult(duplicates=1)
    assert len(calls) == 1  # the handler never re-ran
    # The duplicate was ACKED — it left the queue instead of redelivering forever.
    assert _pending(state) == {}
    assert await q.receive("jobs") == []
    assert q.dead_letters("jobs") == []


# ----------------------- #
# Transient failures + parking


async def test_transient_failure_nacks_then_redelivery_succeeds() -> None:
    ctx, q, state = _mock_harness()
    deliveries: list[int | None] = []

    async def handler(msg: QueueMessage[_Payload]) -> None:
        deliveries.append(msg.delivery_count)

        if len(deliveries) == 1:
            raise RuntimeError("transient")

    await q.enqueue("jobs", _Payload(value="x"), key="evt-1")

    result = await _run(ctx, handler)

    # nack(requeue=True) -> immediate mock redelivery -> success on attempt 2.
    assert deliveries == [1, 2]
    assert result == ConsumerRunResult(processed=1, failed=1)
    assert _pending(state) == {}
    assert q.dead_letters("jobs") == []


# ....................... #


async def test_max_deliveries_parks_poison_without_running_handler() -> None:
    ctx, q, _state = _mock_harness()
    attempts = 0

    async def handler(msg: QueueMessage[_Payload]) -> None:
        del msg
        nonlocal attempts
        attempts += 1
        raise RuntimeError("always fails")

    message_id = await q.enqueue("jobs", _Payload(value="poison"), key="evt-1")

    result = await _run(ctx, handler, max_deliveries=2)

    # Deliveries 1 and 2 ran the handler (and failed); delivery 3 exceeded
    # max_deliveries and was parked WITHOUT a handler attempt.
    assert attempts == 2
    assert result == ConsumerRunResult(failed=2, parked=1)
    assert [m.id for m in q.dead_letters("jobs")] == [message_id]
    assert await q.receive("jobs") == []


# ....................... #


async def test_delivery_at_max_deliveries_still_runs_handler() -> None:
    # Boundary: parking triggers strictly ABOVE max_deliveries, so the
    # handler gets exactly max_deliveries attempts.
    ctx, q, state = _mock_harness()
    deliveries: list[int | None] = []

    async def handler(msg: QueueMessage[_Payload]) -> None:
        deliveries.append(msg.delivery_count)

        if len(deliveries) == 1:
            raise RuntimeError("transient")

    await q.enqueue("jobs", _Payload(value="x"), key="evt-1")

    result = await _run(ctx, handler, max_deliveries=2)

    assert deliveries == [1, 2]  # delivery 2 == max_deliveries -> still ran
    assert result == ConsumerRunResult(processed=1, failed=1)
    assert _pending(state) == {}
    assert q.dead_letters("jobs") == []


# ....................... #


async def test_delivery_count_none_never_parks() -> None:
    # Backend cannot report a count -> parking must never trigger; every
    # failed delivery goes back with requeue=True (broker redrive is the net).
    message = _message("m-1", "evt-1", delivery_count=None)
    stub = _ScriptedQueue(script=[message, message, message])
    ctx = _plain_ctx(state=MockState(), queue_port=stub)
    attempts = 0

    async def handler(msg: QueueMessage[_Payload]) -> None:
        del msg
        nonlocal attempts
        attempts += 1
        raise RuntimeError("always fails")

    result = await _run(ctx, handler, max_deliveries=1)

    assert attempts == 3  # never parked, despite max_deliveries=1
    assert result == ConsumerRunResult(failed=3)
    assert stub.nacked == [("m-1", True)] * 3
    assert stub.acked == []


# ----------------------- #
# Disposition failures never kill the loop


async def test_ack_failure_is_logged_and_loop_continues() -> None:
    stub = _ScriptedQueue(
        script=[
            _message("m-1", "evt-1", delivery_count=1),
            _message("m-2", "evt-2", delivery_count=1),
        ],
        fail_acks_for={"m-1"},
    )
    ctx = _plain_ctx(state=MockState(), queue_port=stub)
    seen: list[str] = []

    async def handler(msg: QueueMessage[_Payload]) -> None:
        seen.append(msg.id)

    result = await _run(ctx, handler)

    # Both processed; the failed ack neither killed the loop nor changed counts
    # (broker redelivery + inbox dedup cover the unacked message).
    assert seen == ["m-1", "m-2"]
    assert result == ConsumerRunResult(processed=2)
    assert stub.acked == ["m-2"]


# ....................... #


async def test_nack_failure_is_logged_and_loop_continues() -> None:
    stub = _ScriptedQueue(
        script=[
            _message("m-1", "evt-1", delivery_count=1),
            _message("m-2", "evt-2", delivery_count=1),
        ],
        fail_nacks_for={"m-1"},
    )
    ctx = _plain_ctx(state=MockState(), queue_port=stub)

    async def handler(msg: QueueMessage[_Payload]) -> None:
        if msg.id == "m-1":
            raise RuntimeError("transient")

    result = await _run(ctx, handler)

    assert result == ConsumerRunResult(processed=1, failed=1)
    assert stub.acked == ["m-2"]
    assert stub.nacked == []  # the one nack attempt exploded; loop went on


# ----------------------- #
# Resilience policy wrapping


async def test_retry_policy_wraps_process_step_before_nack() -> None:
    state = MockState()
    executor = _CountingResilienceExecutor()
    ctx = _plain_ctx(state=state, resilience=executor)
    q = MockQueueAdapter[_Payload](state=state, namespace="jobs", codec=_CODEC)

    attempts = 0

    async def handler(msg: QueueMessage[_Payload]) -> None:
        del msg
        nonlocal attempts
        attempts += 1

        if attempts == 1:
            raise RuntimeError("transient")

    await q.enqueue("jobs", _Payload(value="x"), key="evt-1")

    result = await _run(ctx, handler, retry_policy="consumer-retry")

    # The named policy wrapped the process step; the in-process retry
    # succeeded, so the message never went back to the broker.
    assert executor.policies_used == ["consumer-retry"]
    assert executor.routes_used == ["jobs"]
    assert attempts == 2  # first attempt's inbox mark rolled back with its tx
    assert result == ConsumerRunResult(processed=1)
    assert _pending(state) == {}
    assert q.dead_letters("jobs") == []


# ....................... #


async def test_without_retry_policy_resilience_is_not_touched() -> None:
    state = MockState()
    executor = _CountingResilienceExecutor()
    ctx = _plain_ctx(state=state, resilience=executor)
    q = MockQueueAdapter[_Payload](state=state, namespace="jobs", codec=_CODEC)

    async def handler(msg: QueueMessage[_Payload]) -> None:
        del msg

    await q.enqueue("jobs", _Payload(value="x"), key="evt-1")

    result = await _run(ctx, handler)

    assert result == ConsumerRunResult(processed=1)
    assert executor.policies_used == []


# ....................... #


async def test_retry_policy_without_registered_executor_fails_fast() -> None:
    # The executor resolves BEFORE consuming starts: misconfiguration cannot
    # surface mid-stream.
    stub = _ScriptedQueue(script=[_message("m-1", "evt-1", delivery_count=1)])
    ctx = _plain_ctx(state=MockState(), queue_port=stub)

    async def handler(msg: QueueMessage[_Payload]) -> None:
        del msg

    with pytest.raises(CoreException):
        await _run(ctx, handler, retry_policy="consumer-retry")

    assert stub.acked == [] and stub.nacked == []


# ----------------------- #
# Validation, cancellation, stats


async def test_max_deliveries_below_one_rejected() -> None:
    ctx, _q, _state = _mock_harness()

    async def handler(msg: QueueMessage[_Payload]) -> None:
        del msg

    with pytest.raises(CoreException, match="max_deliveries"):
        await _run(ctx, handler, max_deliveries=0)


# ....................... #


async def test_cancellation_propagates_cleanly() -> None:
    ctx, q, _state = _mock_harness()
    handled = asyncio.Event()

    async def handler(msg: QueueMessage[_Payload]) -> None:
        del msg
        handled.set()

    await q.enqueue("jobs", _Payload(value="x"), key="evt-1")

    task = asyncio.create_task(_run(ctx, handler, timeout=None))  # forever
    await asyncio.wait_for(handled.wait(), timeout=2.0)

    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert task.cancelled()


# ....................... #


async def test_stats_across_mixed_outcomes_in_one_run() -> None:
    ctx, q, state = _mock_harness()
    failed_once = False

    async def handler(msg: QueueMessage[_Payload]) -> None:
        nonlocal failed_once

        if msg.key == "evt-flaky" and not failed_once:
            failed_once = True
            raise RuntimeError("transient")

    await q.enqueue("jobs", _Payload(value="ok"), key="evt-ok")
    await q.enqueue("jobs", _Payload(value="ok"), key="evt-ok")  # duplicate publish
    await q.enqueue("jobs", _Payload(value="flaky"), key="evt-flaky")

    result = await _run(ctx, handler)

    assert result == ConsumerRunResult(processed=2, duplicates=1, failed=1)
    assert _pending(state) == {}
    assert q.dead_letters("jobs") == []
