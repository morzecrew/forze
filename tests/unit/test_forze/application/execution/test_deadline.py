"""Tests for the task-scoped invocation deadline and its enforcement."""

from __future__ import annotations

import asyncio
from datetime import timedelta

import attrs
import pytest

from forze.application.contracts.execution import Handler
from forze.application.execution import (
    ExecutionContext,
    OperationPlan,
    bind_deadline,
    current_deadline,
    remaining_time,
)
from forze.application.execution.operations.registry import OperationRegistry
from forze.base.exceptions import CoreException, ExceptionKind
from forze.base.primitives import str_key_selector
from forze_mock import MockDepsModule
from tests.support.execution_context import context_from_deps

# ----------------------- #


@pytest.fixture
def ctx() -> ExecutionContext:
    return context_from_deps(MockDepsModule()())


@attrs.define(slots=True, kw_only=True, frozen=True)
class EchoHandler(Handler[str, str]):
    async def __call__(self, args: str) -> str:
        return f"handler:{args}"


@attrs.define(slots=True, kw_only=True, frozen=True)
class StallHandler(Handler[str, str]):
    async def __call__(self, args: str) -> str:
        await asyncio.Event().wait()
        return args


# ----------------------- #


class TestDeadlineContext:
    def test_unbound_by_default(self) -> None:
        assert current_deadline() is None
        assert remaining_time() is None

    def test_bind_sets_and_resets(self) -> None:
        with bind_deadline(5.0):
            deadline = current_deadline()
            left = remaining_time()

            assert deadline is not None
            assert left is not None
            assert 0.0 < left <= 5.0

        assert current_deadline() is None
        assert remaining_time() is None

    def test_none_is_passthrough(self) -> None:
        with bind_deadline(None):
            assert current_deadline() is None

        with bind_deadline(5.0):
            outer = current_deadline()

            with bind_deadline(None):
                assert current_deadline() == outer

    def test_nested_bind_tightens(self) -> None:
        with bind_deadline(5.0):
            with bind_deadline(1.0):
                left = remaining_time()
                assert left is not None
                assert left <= 1.0

    def test_nested_bind_never_extends(self) -> None:
        with bind_deadline(1.0):
            outer = current_deadline()

            with bind_deadline(60.0):
                assert current_deadline() == outer

    def test_expired_deadline_clamps_to_zero(self) -> None:
        with bind_deadline(0.0):
            assert remaining_time() == 0.0

    def test_invocation_context_delegates(self, ctx: ExecutionContext) -> None:
        assert ctx.inv_ctx.get_deadline() is None
        assert ctx.inv_ctx.remaining_time() is None

        with ctx.inv_ctx.bind_deadline(5.0):
            assert ctx.inv_ctx.get_deadline() == current_deadline()
            left = ctx.inv_ctx.remaining_time()
            assert left is not None
            assert 0.0 < left <= 5.0


class TestOperationDeadlineEnforcement:
    @pytest.mark.asyncio
    async def test_runs_normally_without_deadline(self, ctx: ExecutionContext) -> None:
        reg = OperationRegistry(handlers={"op": lambda _ctx: EchoHandler()}).freeze()
        resolved = reg.resolve("op", ctx)

        assert await resolved("x") == "handler:x"

    @pytest.mark.asyncio
    async def test_runs_within_deadline(self, ctx: ExecutionContext) -> None:
        reg = OperationRegistry(handlers={"op": lambda _ctx: EchoHandler()}).freeze()
        resolved = reg.resolve("op", ctx)

        with bind_deadline(5.0):
            assert await resolved("x") == "handler:x"

    @pytest.mark.asyncio
    async def test_expired_deadline_fails_fast(self, ctx: ExecutionContext) -> None:
        called: list[str] = []

        @attrs.define(slots=True, kw_only=True, frozen=True)
        class TrackingHandler(Handler[str, str]):
            async def __call__(self, args: str) -> str:
                called.append(args)
                return args

        reg = OperationRegistry(handlers={"op": lambda _ctx: TrackingHandler()}).freeze()
        resolved = reg.resolve("op", ctx)

        with bind_deadline(0.0):
            with pytest.raises(CoreException) as ei:
                await resolved("x")

        assert ei.value.kind is ExceptionKind.TIMEOUT
        assert ei.value.code == "deadline_exceeded"
        assert called == []

    @pytest.mark.asyncio
    async def test_deadline_bounds_running_operation(
        self, ctx: ExecutionContext
    ) -> None:
        reg = OperationRegistry(handlers={"op": lambda _ctx: StallHandler()}).freeze()
        resolved = reg.resolve("op", ctx)

        with bind_deadline(0.05):
            with pytest.raises(CoreException) as ei:
                await resolved("x")

        assert ei.value.kind is ExceptionKind.TIMEOUT
        assert ei.value.code == "deadline_exceeded"


class TestPlanDeadline:
    """Plan-declared deadlines: validation, restrictive merge, enforcement."""

    def test_non_positive_deadline_rejected(self) -> None:
        with pytest.raises(CoreException) as ei:
            OperationPlan(deadline=timedelta(seconds=0))

        assert ei.value.kind is ExceptionKind.CONFIGURATION

    def test_merge_tightest_wins(self) -> None:
        merged = OperationPlan.merge(
            OperationPlan(deadline=timedelta(seconds=10)),
            OperationPlan(),
            OperationPlan(deadline=timedelta(seconds=5)),
        )

        assert merged.deadline == timedelta(seconds=5)

    def test_merge_without_deadlines_stays_unbounded(self) -> None:
        merged = OperationPlan.merge(OperationPlan(), OperationPlan())

        assert merged.deadline is None

    @pytest.mark.asyncio
    async def test_plan_deadline_enforced_without_caller_bind(
        self, ctx: ExecutionContext
    ) -> None:
        reg = (
            OperationRegistry(handlers={"op": lambda _ctx: StallHandler()})
            .bind("op")
            .with_deadline(timedelta(milliseconds=50))
            .finish()
            .freeze()
        )
        resolved = reg.resolve("op", ctx)

        with pytest.raises(CoreException) as ei:
            await resolved("x")

        assert ei.value.kind is ExceptionKind.TIMEOUT
        assert ei.value.code == "deadline_exceeded"

    @pytest.mark.asyncio
    async def test_caller_cannot_extend_plan_deadline(
        self, ctx: ExecutionContext
    ) -> None:
        reg = (
            OperationRegistry(handlers={"op": lambda _ctx: StallHandler()})
            .bind("op")
            .with_deadline(timedelta(milliseconds=50))
            .finish()
            .freeze()
        )
        resolved = reg.resolve("op", ctx)

        async def _invoke() -> str:
            with bind_deadline(60.0):
                return await resolved("x")

        # A generous caller budget must not stretch the plan's 50ms cap.
        with pytest.raises(CoreException) as ei:
            await asyncio.wait_for(_invoke(), timeout=5.0)

        assert ei.value.kind is ExceptionKind.TIMEOUT

    @pytest.mark.asyncio
    async def test_caller_can_tighten_plan_deadline(
        self, ctx: ExecutionContext
    ) -> None:
        reg = (
            OperationRegistry(handlers={"op": lambda _ctx: StallHandler()})
            .bind("op")
            .with_deadline(timedelta(seconds=60))
            .finish()
            .freeze()
        )
        resolved = reg.resolve("op", ctx)

        async def _invoke() -> str:
            with bind_deadline(0.05):
                return await resolved("x")

        with pytest.raises(CoreException) as ei:
            await asyncio.wait_for(_invoke(), timeout=5.0)

        assert ei.value.kind is ExceptionKind.TIMEOUT

    @pytest.mark.asyncio
    async def test_within_plan_deadline_runs_normally(
        self, ctx: ExecutionContext
    ) -> None:
        reg = (
            OperationRegistry(handlers={"op": lambda _ctx: EchoHandler()})
            .bind("op")
            .with_deadline(timedelta(seconds=5))
            .finish()
            .freeze()
        )
        resolved = reg.resolve("op", ctx)

        assert await resolved("x") == "handler:x"

    @pytest.mark.asyncio
    async def test_patch_sets_default_budget(self, ctx: ExecutionContext) -> None:
        reg = (
            OperationRegistry(handlers={"slow.op": lambda _ctx: StallHandler()})
            .patch(str_key_selector.glob("slow.*"))
            .with_deadline(timedelta(milliseconds=50))
            .finish()
            .freeze()
        )
        resolved = reg.resolve("slow.op", ctx)

        with pytest.raises(CoreException) as ei:
            await resolved("x")

        assert ei.value.kind is ExceptionKind.TIMEOUT
        assert ei.value.code == "deadline_exceeded"


class TestDriverDeadlineBudget:
    """Budget handed to a driver-side timeout backstop (Postgres statement_timeout / Mongo CSOT)."""

    def test_none_when_unbound(self) -> None:
        from forze.base.primitives import driver_deadline_budget

        assert driver_deadline_budget() is None

    def test_remaining_plus_grace_when_bound(self) -> None:
        from forze.base.primitives import (
            DEFAULT_DRIVER_DEADLINE_GRACE,
            driver_deadline_budget,
        )

        with bind_deadline(5.0):
            budget = driver_deadline_budget()
            remaining = remaining_time()

        assert budget is not None and remaining is not None
        # Looser than the authoritative asyncio deadline (so that fires first).
        assert budget > remaining
        assert budget == pytest.approx(
            remaining + DEFAULT_DRIVER_DEADLINE_GRACE, abs=0.05
        )

    def test_positive_even_at_expiry(self) -> None:
        # A (near-)expired deadline still yields a positive budget (the grace), never 0 —
        # a driver timeout of 0 means *unlimited*, the opposite of intended.
        from forze.base.primitives import driver_deadline_budget

        with bind_deadline(0.0):
            budget = driver_deadline_budget()

        assert budget is not None and budget > 0.0

    def test_custom_grace(self) -> None:
        from forze.base.primitives import driver_deadline_budget

        with bind_deadline(5.0):
            zero = driver_deadline_budget(grace=0.0)
            one = driver_deadline_budget(grace=1.0)

        assert zero is not None and one is not None
        assert one == pytest.approx(zero + 1.0, abs=0.05)
