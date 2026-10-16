"""Unit tests for the relay's pass scope, its budget and the publish timeout.

# covers: forze_kits.integrations.outbox._relay_core.relay_outbox_claims
# covers: forze.application.contracts.outbox.OutboxQueryPort.pass_scope
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.outbox import (
    OutboxClaim,
    OutboxPassBudget,
    OutboxQueryPort,
    OutboxSpec,
    OutboxStatus,
)
from forze.application.execution import DepsRegistry, ExecutionRuntime
from forze.application.execution.context import ExecutionContext
from forze.base.exceptions import CoreException
from forze.base.serialization import PydanticModelCodec
from forze_kits.integrations.outbox import OutboxRelay
from forze_kits.integrations.outbox._relay_core import _pass_scope, relay_outbox_claims
from forze_mock import MockDepsModule, MockStateDepKey
from forze_mock.adapters import MockState
from forze_mock.adapters.outbox import MockOutboxRow, MockOutboxStore

# ----------------------- #


class _Payload(BaseModel):
    n: int


_T0 = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
_SPEC: OutboxSpec[_Payload] = OutboxSpec(name="events", codec=PydanticModelCodec(_Payload))


def _seed(state: MockState, count: int) -> list[MockOutboxRow]:
    rows = [
        MockOutboxRow(
            id=uuid4(),
            outbox_route="events",
            event_id=uuid4(),
            event_type="job.requested",
            payload={"n": index},
            status=OutboxStatus.PENDING,
            tenant_id=None,
            execution_id=None,
            correlation_id=None,
            causation_id=None,
            occurred_at=_T0,
            created_at=_T0 + timedelta(microseconds=index),
        )
        for index in range(count)
    ]
    state.outbox_rows.setdefault("events", []).extend(rows)
    return rows


@asynccontextmanager
async def _context() -> AsyncIterator[tuple[ExecutionContext, MockState]]:
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(MockDepsModule()).freeze())

    async with runtime.scope():
        ctx = runtime.get_context()
        yield ctx, ctx.deps.provide(MockStateDepKey)


def _budget_scope(deadline_in: float, entered: list[str] | None = None) -> Any:
    """A pass scope that yields a budget ending *deadline_in* seconds after it is entered."""

    @asynccontextmanager
    async def scope(_self: Any) -> AsyncGenerator[OutboxPassBudget]:
        if entered is not None:
            entered.append("enter")

        yield OutboxPassBudget(deadline=asyncio.get_running_loop().time() + deadline_in)

        if entered is not None:
            entered.append("exit")

    return scope


async def _relay(ctx: ExecutionContext, publish_one: Any, **kwargs: Any) -> Any:
    return await relay_outbox_claims(
        ctx,
        outbox_spec=_SPEC,
        publish_one=publish_one,
        reclaim_stale_after=None,
        **kwargs,
    )


async def _publish_ok(claim: OutboxClaim, payload: Any) -> None:
    del claim, payload


# ....................... #


class TestRelayPassScope:
    async def test_the_default_scope_is_a_no_op(self) -> None:
        async with _context() as (ctx, _state):
            store = ctx.outbox.query(_SPEC)

            async with store.pass_scope() as budget:
                assert budget is None

            assert store.transactional_passes is False

    async def test_a_store_without_a_pass_scope_gets_the_no_op(self) -> None:
        # A store implementing the protocol structurally predates the method.
        async with _pass_scope(object()) as budget:  # type: ignore[arg-type]
            assert budget is None

    async def test_one_pass_enters_the_scope_once_around_its_claim_and_marks(self) -> None:
        entered: list[str] = []
        seen_inside: list[bool] = []

        async def publish(claim: OutboxClaim, payload: Any) -> None:
            seen_inside.append(entered == ["enter"])

        async with _context() as (ctx, state):
            _seed(state, 3)

            with patch.object(MockOutboxStore, "pass_scope", _budget_scope(60.0, entered)):
                result = await _relay(ctx, publish)

        assert entered == ["enter", "exit"]
        assert seen_inside == [True, True, True]
        assert result.published == 3


class TestRelayPassScopeDoubles:
    async def test_a_test_doubles_stand_in_budget_bounds_nothing(self) -> None:
        # MagicMock(spec=OutboxQueryPort).pass_scope() yields a MagicMock, not a budget.
        double = MagicMock(spec=OutboxQueryPort)

        async with _context() as (ctx, state):
            _seed(state, 2)

            with patch.object(MockOutboxStore, "pass_scope", lambda _self: double.pass_scope()):
                result = await _relay(ctx, _publish_ok)

        assert result.published == 2


class TestRelayPassBudget:
    async def test_a_spent_budget_leaves_every_claimed_row_unchanged(self) -> None:
        calls: list[OutboxClaim] = []

        async def publish(claim: OutboxClaim, payload: Any) -> None:
            calls.append(claim)

        async with _context() as (ctx, state):
            rows = _seed(state, 3)

            with patch.object(MockOutboxStore, "pass_scope", _budget_scope(-1.0)):
                result = await _relay(ctx, publish)

        assert calls == []
        assert (result.claimed, result.published, result.retried, result.failed) == (3, 0, 0, 0)
        assert all(row.attempts == 0 and row.last_error is None for row in rows)

    async def test_the_deadline_cuts_the_publish_in_flight_and_stops_the_pass(self) -> None:
        async def publish(claim: OutboxClaim, payload: Any) -> None:
            if payload.n == 1:
                await asyncio.sleep(30)  # a destination slower than the pass allows

        async with _context() as (ctx, state):
            rows = _seed(state, 3)

            with patch.object(MockOutboxStore, "pass_scope", _budget_scope(0.3)):
                result = await asyncio.wait_for(_relay(ctx, publish), timeout=10)

        assert (result.published, result.retried, result.failed) == (1, 1, 0)
        first, cut, unreached = rows
        assert first.status == OutboxStatus.PUBLISHED
        # The cut publish is a transient failure: it keeps its attempt and backoff.
        assert cut.attempts == 1 and cut.available_at is not None
        assert cut.last_error is not None and cut.last_error.startswith("publish timed out")
        # The row after the deadline was never touched.
        assert unreached.attempts == 0 and unreached.last_error is None


class TestRelayPublishUntil:
    async def test_it_ends_publishing_before_a_later_budget_does(self) -> None:
        calls: list[OutboxClaim] = []

        async def publish(claim: OutboxClaim, payload: Any) -> None:
            calls.append(claim)

        async with _context() as (ctx, state):
            rows = _seed(state, 2)

            with patch.object(MockOutboxStore, "pass_scope", _budget_scope(60.0)):
                result = await _relay(
                    ctx, publish, publish_until=asyncio.get_running_loop().time() - 1
                )

        assert calls == []
        assert (result.claimed, result.published) == (2, 0)
        assert all(row.attempts == 0 for row in rows)


class TestRelayPublishTimeout:
    async def test_a_slow_publish_is_retried_and_the_rest_publish(self) -> None:
        async def publish(claim: OutboxClaim, payload: Any) -> None:
            if payload.n == 0:
                await asyncio.sleep(30)

        async with _context() as (ctx, state):
            rows = _seed(state, 3)
            result = await asyncio.wait_for(
                _relay(ctx, publish, publish_timeout=timedelta(milliseconds=50)),
                timeout=10,
            )

        assert (result.published, result.retried) == (2, 1)
        assert rows[0].attempts == 1
        assert rows[0].last_error == "publish timed out after 0.050s"
        assert [row.status for row in rows[1:]] == [OutboxStatus.PUBLISHED] * 2

    async def test_a_slow_publish_at_its_last_attempt_is_parked(self) -> None:
        async def publish(claim: OutboxClaim, payload: Any) -> None:
            await asyncio.sleep(30)

        async with _context() as (ctx, state):
            [row] = _seed(state, 1)
            result = await asyncio.wait_for(
                _relay(ctx, publish, max_attempts=1, publish_timeout=timedelta(milliseconds=20)),
                timeout=10,
            )

        assert result.failed == 1
        assert row.status == OutboxStatus.FAILED

    @pytest.mark.parametrize("timeout", [timedelta(0), timedelta(seconds=-1)])
    def test_a_timeout_that_is_not_positive_is_refused(self, timeout: timedelta) -> None:
        with pytest.raises(CoreException, match="publish_timeout"):
            OutboxRelay(outbox_spec=_SPEC, publish_timeout=timeout)

    async def test_a_publish_error_keeps_its_own_message(self) -> None:
        async def publish(claim: OutboxClaim, payload: Any) -> None:
            raise RuntimeError("broker down")

        async with _context() as (ctx, state):
            [row] = _seed(state, 1)
            await _relay(ctx, publish, publish_timeout=timedelta(seconds=5))

        assert row.last_error == "broker down"
