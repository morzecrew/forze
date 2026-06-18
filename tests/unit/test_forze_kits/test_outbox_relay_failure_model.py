"""Unit tests for the outbox relay failure model.

# covers: forze_kits.integrations.outbox._relay_core.relay_outbox_claims
# covers: forze_kits.integrations.outbox._relay_core.compute_retry_delay
# covers: forze_kits.integrations.outbox._relay_core.validate_retry_options

Poison rows (decode errors) are marked ``failed`` immediately with attempts
untouched; transient publish errors are rescheduled via ``mark_retry`` with
exponential backoff until ``max_attempts``, then marked ``failed`` (terminal).
Time is controlled through the ``TimeSource`` seam (``bind_time_source`` +
``FrozenTimeSource``) so backoff visibility is asserted without sleeping.
"""

from __future__ import annotations

import random
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.outbox import (
    OutboxClaim,
    OutboxSpec,
    OutboxStatus,
)
from forze.application.execution import DepsRegistry, ExecutionRuntime
from forze.application.execution.context import ExecutionContext
from forze.base.exceptions import CoreException
from forze.base.primitives import FrozenTimeSource, bind_time_source
from forze.base.serialization import PydanticModelCodec
from forze_kits.integrations.outbox._relay_core import (
    compute_retry_delay,
    relay_outbox_claims,
    validate_retry_options,
)
from forze_mock import MockDepsModule, MockStateDepKey
from forze_mock.adapters import MockState
from forze_mock.adapters.outbox import MockOutboxRow, MockOutboxStore

# ----------------------- #


class _EventPayload(BaseModel):
    n: int


_T0 = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)


def _outbox_spec() -> OutboxSpec[_EventPayload]:
    return OutboxSpec(name="events", codec=PydanticModelCodec(_EventPayload))


def _row(
    payload: dict[str, Any],
    *,
    index: int = 0,
    status: OutboxStatus = OutboxStatus.PENDING,
    attempts: int = 0,
    event_type: str = "job.requested",
) -> MockOutboxRow:
    return MockOutboxRow(
        id=uuid4(),
        outbox_route="events",
        event_id=uuid4(),
        event_type=event_type,
        payload=payload,
        status=status,
        tenant_id=None,
        execution_id=None,
        correlation_id=None,
        causation_id=None,
        occurred_at=_T0,
        created_at=_T0 + timedelta(microseconds=index),
        attempts=attempts,
    )


@asynccontextmanager
async def _runtime_ctx() -> AsyncIterator[tuple[ExecutionContext, MockState]]:
    runtime = ExecutionRuntime(
        deps=DepsRegistry.from_modules(MockDepsModule()).freeze()
    )
    async with runtime.scope():
        ctx = runtime.get_context()
        state = ctx.deps.provide(MockStateDepKey)
        yield ctx, state


def _always_fail(message: str = "broker down"):
    async def publish(claim: OutboxClaim, payload: Any) -> None:
        raise RuntimeError(message)

    return publish


def _recorder(published: list[Any]):
    async def publish(claim: OutboxClaim, payload: Any) -> None:
        published.append(payload)

    return publish


async def _relay(
    ctx: ExecutionContext,
    publish_one: Any,
    *,
    max_attempts: int = 5,
    retry_base_delay: timedelta = timedelta(seconds=1),
    retry_max_backoff: timedelta = timedelta(seconds=60),
):
    return await relay_outbox_claims(
        ctx,
        outbox_spec=_outbox_spec(),
        publish_one=publish_one,
        reclaim_stale_after=None,
        max_attempts=max_attempts,
        retry_base_delay=retry_base_delay,
        retry_max_backoff=retry_max_backoff,
    )


# ----------------------- #
# Transient failures


@pytest.mark.asyncio
async def test_transient_failure_reschedules_row_with_backoff() -> None:
    clock = FrozenTimeSource(instant=_T0)

    async with _runtime_ctx() as (ctx, state):
        row = _row({"n": 7})
        state.outbox_rows["events"] = [row]

        with bind_time_source(clock):
            result = await _relay(ctx, _always_fail("queue unreachable"))

        assert result.claimed == 1
        assert result.retried == 1
        assert result.published == 0
        assert result.failed == 0

        assert row.status == OutboxStatus.PENDING
        assert row.attempts == 1
        assert row.last_error is not None and "queue unreachable" in row.last_error
        # Equal jitter: delay in [base / 2, base] for the first retry.
        assert row.available_at is not None
        assert (
            _T0 + timedelta(seconds=0.5)
            <= row.available_at
            <= _T0 + timedelta(seconds=1)
        )


@pytest.mark.asyncio
async def test_rescheduled_row_is_invisible_until_available_at() -> None:
    clock = FrozenTimeSource(instant=_T0)
    published: list[Any] = []

    async with _runtime_ctx() as (ctx, state):
        row = _row({"n": 7})
        state.outbox_rows["events"] = [row]

        with bind_time_source(clock):
            first = await _relay(ctx, _always_fail())
            # Same frozen instant: the rescheduled row must not be claimable.
            second = await _relay(ctx, _recorder(published))

        assert first.retried == 1
        assert second.claimed == 0
        assert second.published == 0
        assert published == []
        assert row.status == OutboxStatus.PENDING


@pytest.mark.asyncio
async def test_row_claimable_again_after_available_at_passes() -> None:
    clock = FrozenTimeSource(instant=_T0)
    published: list[Any] = []

    async with _runtime_ctx() as (ctx, state):
        row = _row({"n": 7})
        state.outbox_rows["events"] = [row]

        with bind_time_source(clock):
            await _relay(ctx, _always_fail())

            # Advance the frozen clock past the maximum possible backoff.
            clock.instant = _T0 + timedelta(seconds=2)
            result = await _relay(ctx, _recorder(published))

        assert result.claimed == 1
        assert result.published == 1
        assert row.status == OutboxStatus.PUBLISHED
        assert published == [_EventPayload(n=7)]


@pytest.mark.asyncio
async def test_attempts_exhaustion_marks_failed_terminal() -> None:
    async with _runtime_ctx() as (ctx, state):
        row = _row({"n": 7}, attempts=2)
        state.outbox_rows["events"] = [row]

        result = await _relay(ctx, _always_fail("still down"), max_attempts=3)

        assert result.claimed == 1
        assert result.failed == 1
        assert result.retried == 0
        assert result.published == 0

        assert row.status == OutboxStatus.FAILED
        assert row.last_error is not None and "still down" in row.last_error


# ----------------------- #
# Poison rows and batch isolation


@pytest.mark.asyncio
async def test_poison_row_fails_immediately_without_attempt_bump() -> None:
    published: list[Any] = []

    async with _runtime_ctx() as (ctx, state):
        poison = _row({"not_n": "bad"}, index=1)
        state.outbox_rows["events"] = [
            _row({"n": 1}, index=0),
            poison,
            _row({"n": 3}, index=2),
        ]

        result = await _relay(ctx, _recorder(published))

        assert result.claimed == 3
        assert result.published == 2
        assert result.failed == 1
        assert result.retried == 0

        assert poison.status == OutboxStatus.FAILED
        assert poison.attempts == 0
        assert poison.last_error is not None
        assert published == [_EventPayload(n=1), _EventPayload(n=3)]


@pytest.mark.asyncio
async def test_transient_failure_does_not_abort_rest_of_batch() -> None:
    published: list[Any] = []

    async def publish(claim: OutboxClaim, payload: Any) -> None:
        if payload.n == 2:
            raise RuntimeError("broker hiccup")
        published.append(payload)

    async with _runtime_ctx() as (ctx, state):
        flaky = _row({"n": 2}, index=1)
        state.outbox_rows["events"] = [
            _row({"n": 1}, index=0),
            flaky,
            _row({"n": 3}, index=2),
        ]

        result = await _relay(ctx, publish)

        assert result.claimed == 3
        assert result.published == 2
        assert result.retried == 1
        assert result.failed == 0
        assert flaky.status == OutboxStatus.PENDING
        assert flaky.attempts == 1
        assert published == [_EventPayload(n=1), _EventPayload(n=3)]


# ----------------------- #
# Batched marking


def _spy_mark(
    monkeypatch: pytest.MonkeyPatch,
    method: str,
) -> list[tuple[list[Any], dict[str, Any]]]:
    """Record ``(ids, kwargs)`` per call to *method* on the mock outbox store."""

    calls: list[tuple[list[Any], dict[str, Any]]] = []
    original = getattr(MockOutboxStore, method)

    async def wrapper(self: Any, ids: Any, **kwargs: Any) -> int:
        calls.append((list(ids), dict(kwargs)))
        return await original(self, ids, **kwargs)

    monkeypatch.setattr(MockOutboxStore, method, wrapper)
    return calls


@pytest.mark.asyncio
async def test_mark_published_flushes_in_chunks_of_32(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = _spy_mark(monkeypatch, "mark_published")
    published: list[Any] = []

    async with _runtime_ctx() as (ctx, state):
        rows = [_row({"n": i}, index=i) for i in range(100)]
        state.outbox_rows["events"] = rows

        result = await _relay(ctx, _recorder(published))

        assert result.claimed == 100
        assert result.published == 100
        assert len(published) == 100

        # ceil(100 / 32) = 4 flushes, partitioned in claim order.
        assert [len(ids) for ids, _ in calls] == [32, 32, 32, 4]
        flat = [row_id for ids, _ in calls for row_id in ids]
        assert flat == [r.id for r in rows]
        assert all(r.status == OutboxStatus.PUBLISHED for r in rows)


@pytest.mark.asyncio
async def test_mixed_batch_chunks_published_groups_retries_fails_per_row(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    published_calls = _spy_mark(monkeypatch, "mark_published")
    retry_calls = _spy_mark(monkeypatch, "mark_retry")
    failed_calls = _spy_mark(monkeypatch, "mark_failed")

    async def publish(claim: OutboxClaim, payload: Any) -> None:
        if payload.n < 0:
            raise RuntimeError(f"broker down {payload.n}")

    async with _runtime_ctx() as (ctx, state):
        ok_rows = [_row({"n": i}, index=i) for i in range(40)]
        # Two transient rows with attempts=0 (-> group attempts=1, errors
        # differ) and one with attempts=1 (-> group attempts=2).
        transient_first = [
            _row({"n": -1}, index=100),
            _row({"n": -2}, index=101),
        ]
        transient_second = [_row({"n": -3}, index=102, attempts=1)]
        poison = [
            _row({"not_n": "x"}, index=103),
            _row({"not_n": "y"}, index=104),
        ]
        state.outbox_rows["events"] = (
            ok_rows + transient_first + transient_second + poison
        )

        result = await _relay(ctx, publish)

        assert result.claimed == 45
        assert result.published == 40
        assert result.retried == 3
        assert result.failed == 2

        # Published ids flushed in chunks (claim order preserved).
        assert [len(ids) for ids, _ in published_calls] == [32, 8]
        flat = [row_id for ids, _ in published_calls for row_id in ids]
        assert flat == [r.id for r in ok_rows]

        # One mark_retry per attempts group.
        assert len(retry_calls) == 2
        by_attempts = {kw["attempts"]: (ids, kw) for ids, kw in retry_calls}

        ids_1, kw_1 = by_attempts[1]
        assert set(ids_1) == {r.id for r in transient_first}
        # Rows in one group share the single jittered available_at.
        assert (
            transient_first[0].available_at
            == transient_first[1].available_at
            == kw_1["available_at"]
        )
        # First error kept; differing errors noted as "+N more".
        assert "broker down -1" in kw_1["error"]
        assert "(+1 more)" in kw_1["error"]

        ids_2, kw_2 = by_attempts[2]
        assert ids_2 == [transient_second[0].id]
        assert kw_2["error"] == "broker down -3"
        assert transient_second[0].attempts == 2

        # Poison rows stay per-row with their own error.
        assert [len(ids) for ids, _ in failed_calls] == [1, 1]
        assert all(r.status == OutboxStatus.FAILED for r in poison)
        errors = {kw["error"] for _, kw in failed_calls}
        assert len(errors) == 2  # per-row error fidelity preserved


@pytest.mark.asyncio
async def test_grouped_retry_identical_errors_not_annotated(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    retry_calls = _spy_mark(monkeypatch, "mark_retry")

    async with _runtime_ctx() as (ctx, state):
        rows = [_row({"n": i}, index=i) for i in range(3)]
        state.outbox_rows["events"] = rows

        await _relay(ctx, _always_fail("broker down"))

        assert len(retry_calls) == 1
        _, kw = retry_calls[0]
        assert kw["error"] == "broker down"
        assert "more" not in kw["error"]


@pytest.mark.asyncio
async def test_publish_failure_mid_batch_does_not_lose_other_marks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    published_calls = _spy_mark(monkeypatch, "mark_published")

    async def publish(claim: OutboxClaim, payload: Any) -> None:
        if payload.n == 10:
            raise RuntimeError("broker hiccup")

    async with _runtime_ctx() as (ctx, state):
        rows = [_row({"n": i}, index=i) for i in range(35)]
        state.outbox_rows["events"] = rows

        result = await _relay(ctx, publish)

        assert result.published == 34
        assert result.retried == 1
        # 34 successes flush as a full chunk plus the remainder.
        assert [len(ids) for ids, _ in published_calls] == [32, 2]
        assert sum(r.status == OutboxStatus.PUBLISHED for r in rows) == 34
        assert rows[10].status == OutboxStatus.PENDING
        assert rows[10].attempts == 1


@pytest.mark.asyncio
async def test_mark_published_flush_failure_propagates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def boom(self: Any, ids: Any) -> int:
        raise RuntimeError("mark flush down")

    monkeypatch.setattr(MockOutboxStore, "mark_published", boom)

    async with _runtime_ctx() as (ctx, state):
        rows = [_row({"n": i}, index=i) for i in range(3)]
        state.outbox_rows["events"] = rows

        with pytest.raises(RuntimeError, match="mark flush down"):
            await _relay(ctx, _recorder([]))

        # Published-but-unmarked rows stay processing -> reclaim/redeliver.
        assert all(r.status == OutboxStatus.PROCESSING for r in rows)


@pytest.mark.asyncio
async def test_mark_retry_flush_failure_propagates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def boom(self: Any, ids: Any, **kwargs: Any) -> int:
        raise RuntimeError("retry flush down")

    monkeypatch.setattr(MockOutboxStore, "mark_retry", boom)

    async with _runtime_ctx() as (ctx, state):
        row = _row({"n": 1})
        state.outbox_rows["events"] = [row]

        with pytest.raises(RuntimeError, match="retry flush down"):
            await _relay(ctx, _always_fail())

        assert row.status == OutboxStatus.PROCESSING


# ----------------------- #
# Operator re-drive


@pytest.mark.asyncio
async def test_requeue_failed_resets_attempts_and_republishes() -> None:
    published: list[Any] = []

    async with _runtime_ctx() as (ctx, state):
        row = _row({"n": 9}, status=OutboxStatus.FAILED, attempts=5)
        row.last_error = "exhausted"
        row.available_at = _T0 + timedelta(hours=1)
        state.outbox_rows["events"] = [row]

        query = ctx.outbox.query(_outbox_spec())
        assert await query.requeue_failed([row.id]) == 1

        assert row.status == OutboxStatus.PENDING
        assert row.attempts == 0
        assert row.available_at is None
        assert row.last_error is None

        result = await _relay(ctx, _recorder(published))

        assert result.published == 1
        assert row.status == OutboxStatus.PUBLISHED
        assert published == [_EventPayload(n=9)]


# ----------------------- #
# Backoff math


def test_compute_retry_delay_exponential_within_jitter_band() -> None:
    rng = random.Random(42)
    base = timedelta(seconds=1)
    cap = timedelta(seconds=60)

    for attempt in range(1, 12):
        raw = min(60.0, 2.0 ** (attempt - 1))
        delay = compute_retry_delay(
            attempt,
            retry_base_delay=base,
            retry_max_backoff=cap,
            rng=rng,
        ).total_seconds()

        assert raw / 2 <= delay <= raw

    # The jitter band itself grows exponentially: the floor of attempt 5
    # already exceeds the ceiling of attempt 1.
    a1 = compute_retry_delay(
        1, retry_base_delay=base, retry_max_backoff=cap, rng=rng
    ).total_seconds()
    a5 = compute_retry_delay(
        5, retry_base_delay=base, retry_max_backoff=cap, rng=rng
    ).total_seconds()
    assert a5 > a1


def test_compute_retry_delay_caps_at_max_backoff_for_large_attempts() -> None:
    rng = random.Random(7)
    cap = timedelta(seconds=30)

    for attempt in (10, 100, 1000):
        delay = compute_retry_delay(
            attempt,
            retry_base_delay=timedelta(seconds=1),
            retry_max_backoff=cap,
            rng=rng,
        ).total_seconds()

        assert 15.0 <= delay <= 30.0


# ----------------------- #
# Option validation


@pytest.mark.parametrize(
    ("max_attempts", "base", "cap"),
    [
        (0, timedelta(seconds=1), timedelta(seconds=60)),
        (-1, timedelta(seconds=1), timedelta(seconds=60)),
        (5, timedelta(seconds=0), timedelta(seconds=60)),
        (5, timedelta(seconds=-1), timedelta(seconds=60)),
        (5, timedelta(seconds=10), timedelta(seconds=1)),
    ],
)
def test_validate_retry_options_rejects_invalid(
    max_attempts: int,
    base: timedelta,
    cap: timedelta,
) -> None:
    with pytest.raises(CoreException):
        validate_retry_options(
            max_attempts=max_attempts,
            retry_base_delay=base,
            retry_max_backoff=cap,
        )


@pytest.mark.asyncio
async def test_relay_rejects_invalid_retry_options() -> None:
    async with _runtime_ctx() as (ctx, _):
        with pytest.raises(CoreException, match="max_attempts"):
            await _relay(ctx, _always_fail(), max_attempts=0)

        with pytest.raises(CoreException, match="retry_base_delay"):
            await _relay(ctx, _always_fail(), retry_base_delay=timedelta(0))

        with pytest.raises(CoreException, match="retry_max_backoff"):
            await _relay(
                ctx,
                _always_fail(),
                retry_base_delay=timedelta(seconds=10),
                retry_max_backoff=timedelta(seconds=1),
            )
