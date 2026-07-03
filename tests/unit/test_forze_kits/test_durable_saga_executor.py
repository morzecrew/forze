"""Durable saga executor: journaled steps, reverse compensation, crash resume, guards.

# covers: DurableSagaExecutor.run
# covers: durable_saga_handler
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any, cast

import pytest
from pydantic import BaseModel

from forze.application.contracts.durable.function import (
    DurableRunContext,
    DurableRunStatus,
    bind_durable_run,
    reset_durable_run,
)
from forze.application.contracts.saga import SagaDefinition, SagaStep, SagaStepKind
from forze.application.execution import ExecutionContext
from forze.base.exceptions import CoreException, ExceptionKind, exc
from forze.base.primitives import utcnow
from tests.support.execution_context import context_from_modules

from forze_kits.integrations.durable import (
    DurableFunctionRegistry,
    DurableFunctionRunner,
    DurableSagaExecutor,
    durable_saga_handler,
    resolve_durable_run_store,
    resolve_durable_step,
)
from forze_mock import MockDepsModule, MockState

# ----------------------- #


class OrderCtx(BaseModel):
    trail: list[str] = []


def _step(
    name: str,
    effects: list[str],
    *,
    fail: bool = False,
    comp: bool = True,
    kind: SagaStepKind = SagaStepKind.COMPENSATABLE,
) -> SagaStep[OrderCtx]:
    async def action(_ctx: ExecutionContext, state: OrderCtx) -> OrderCtx:
        effects.append(f"do:{name}")
        if fail:
            raise exc.infrastructure(f"{name} failed")
        return OrderCtx(trail=[*state.trail, name])

    async def compensation(_ctx: ExecutionContext, _state: OrderCtx) -> None:
        effects.append(f"undo:{name}")

    return SagaStep(
        name=name,
        action=action,
        compensation=compensation if comp else None,
        kind=kind,
        tx_route="mock",
    )


def _bound(run_id: str = "r1"):
    return bind_durable_run(DurableRunContext(run_id=run_id, name="order"))


# ....................... #


class TestDurableSagaExecutor:
    async def test_happy_path_runs_and_journals_all_steps(self) -> None:
        ctx = context_from_modules(MockDepsModule())
        effects: list[str] = []
        saga: SagaDefinition[OrderCtx] = SagaDefinition(
            name="order",
            steps=(_step("a", effects), _step("b", effects), _step("c", effects)),
        )

        token = _bound()
        try:
            result = await DurableSagaExecutor().run(ctx, saga, OrderCtx())
        finally:
            reset_durable_run(token)

        assert result.trail == ["a", "b", "c"]
        assert effects == ["do:a", "do:b", "do:c"]

    async def test_replay_under_same_run_skips_completed_steps(self) -> None:
        # One MockState across both invocations so the journal persists between them.
        state = MockState()
        effects: list[str] = []
        saga: SagaDefinition[OrderCtx] = SagaDefinition(
            name="order",
            steps=(_step("a", effects), _step("b", effects)),
        )
        executor = DurableSagaExecutor()

        ctx1 = context_from_modules(MockDepsModule(state=state))
        token = _bound()
        try:
            await executor.run(ctx1, saga, OrderCtx())
        finally:
            reset_durable_run(token)

        ctx2 = context_from_modules(MockDepsModule(state=state))
        token = _bound()
        try:
            replayed = await executor.run(ctx2, saga, OrderCtx())
        finally:
            reset_durable_run(token)

        assert replayed.trail == ["a", "b"]
        assert effects == ["do:a", "do:b"]  # unchanged — steps replayed from the journal

    async def test_failure_compensates_completed_steps_in_reverse(self) -> None:
        ctx = context_from_modules(MockDepsModule())
        effects: list[str] = []
        saga: SagaDefinition[OrderCtx] = SagaDefinition(
            name="order",
            steps=(
                _step("a", effects),
                _step("b", effects),
                _step("c", effects, fail=True),
            ),
        )

        token = _bound()
        try:
            with pytest.raises(CoreException) as ei:
                await DurableSagaExecutor().run(ctx, saga, OrderCtx())
        finally:
            reset_durable_run(token)

        assert ei.value.kind is ExceptionKind.DOMAIN  # consistent: compensated
        assert effects == ["do:a", "do:b", "do:c", "undo:b", "undo:a"]

    async def test_outside_a_durable_run_is_rejected(self) -> None:
        ctx = context_from_modules(MockDepsModule())
        effects: list[str] = []
        saga: SagaDefinition[OrderCtx] = SagaDefinition(
            name="order", steps=(_step("a", effects),)
        )

        with pytest.raises(CoreException, match="durable run"):
            await DurableSagaExecutor().run(ctx, saga, OrderCtx())

    async def test_non_serializable_context_is_rejected(self) -> None:
        ctx = context_from_modules(MockDepsModule())
        effects: list[str] = []
        saga: SagaDefinition[OrderCtx] = SagaDefinition(
            name="order", steps=(_step("a", effects),)
        )

        token = _bound()
        try:
            with pytest.raises(CoreException, match="BaseModel"):
                await DurableSagaExecutor().run(ctx, saga, cast(Any, ["not-a-model"]))
        finally:
            reset_durable_run(token)


class TestDurableSagaCrashRecovery:
    async def test_crash_mid_saga_resumes_without_reexecuting_committed_steps(
        self,
    ) -> None:
        state = MockState()
        effects: list[str] = []
        saga: SagaDefinition[OrderCtx] = SagaDefinition(
            name="order",
            steps=(_step("a", effects), _step("b", effects)),
        )
        registry = DurableFunctionRegistry()
        registry.register("order", durable_saga_handler(saga, OrderCtx))
        runner = DurableFunctionRunner(registry=registry)

        ctx = context_from_modules(MockDepsModule(state=state))
        store = resolve_durable_run_store(ctx)

        record = await store.enqueue(
            "order", input_json=OrderCtx().model_dump(mode="json")
        )
        await store.begin(record.run_id, lease_for=timedelta(minutes=5))

        # Simulate a crash after step "a": journal it exactly as the executor would (the
        # encoded post-step context), leaving the run RUNNING.
        token = bind_durable_run(
            DurableRunContext(run_id=record.run_id, name="order")
        )
        try:
            step_port = resolve_durable_step(ctx)

            async def journal_a() -> dict:
                effects.append("do:a")
                return {"trail": ["a"]}

            await step_port.run("a", journal_a)
        finally:
            reset_durable_run(token)

        assert effects == ["do:a"]

        # Expire the crashed run's lease and recover it.
        state.durable_runs[record.run_id]["leased_until"] = utcnow() - timedelta(hours=1)
        assert await runner.recover(ctx) == 1

        reloaded = await store.load(record.run_id)
        assert reloaded is not None
        assert reloaded.status is DurableRunStatus.COMPLETED
        # step "a" replayed from the journal (no second "do:a"); "b" ran live to completion.
        assert effects == ["do:a", "do:b"]
        assert reloaded.output_json == {"trail": ["a", "b"]}


# ....................... #


def _flex_step(
    name: str,
    effects: list[str],
    *,
    tx_route: str | None = "mock",
    fail: bool = False,
    comp: str = "ok",  # "ok" | "none" | "raise"
    kind: SagaStepKind = SagaStepKind.COMPENSATABLE,
    idempotent: bool = False,
) -> SagaStep[OrderCtx]:
    async def action(_ctx: ExecutionContext, state: OrderCtx) -> OrderCtx:
        effects.append(f"do:{name}")
        if fail:
            raise exc.infrastructure(f"{name} failed")
        return OrderCtx(trail=[*state.trail, name])

    async def compensation(_ctx: ExecutionContext, _state: OrderCtx) -> None:
        effects.append(f"undo:{name}")
        if comp == "raise":
            raise exc.infrastructure(f"{name} undo failed")

    return SagaStep(
        name=name,
        action=action,
        compensation=compensation if comp != "none" else None,
        kind=kind,
        tx_route=tx_route,
        idempotent=idempotent,
    )


class TestDurableSagaExecutorEdges:
    async def test_running_inside_a_transaction_is_rejected(self) -> None:
        ctx = context_from_modules(MockDepsModule())
        saga: SagaDefinition[OrderCtx] = SagaDefinition(
            name="order", steps=(_flex_step("a", []),)
        )

        token = _bound()
        try:
            with pytest.raises(CoreException, match="outside a transaction"):
                async with ctx.tx_ctx.scope("mock"):
                    await DurableSagaExecutor().run(ctx, saga, OrderCtx())
        finally:
            reset_durable_run(token)

    async def test_failure_after_pivot_is_forward_incomplete_not_compensated(
        self,
    ) -> None:
        ctx = context_from_modules(MockDepsModule())
        effects: list[str] = []
        saga: SagaDefinition[OrderCtx] = SagaDefinition(
            name="order",
            steps=(
                _flex_step("a", effects),
                _flex_step("p", effects, kind=SagaStepKind.PIVOT),
                _flex_step(
                    "c",
                    effects,
                    kind=SagaStepKind.RETRYABLE,
                    idempotent=True,
                    fail=True,
                ),
            ),
        )

        token = _bound()
        try:
            with pytest.raises(CoreException) as ei:
                await DurableSagaExecutor().run(ctx, saga, OrderCtx())
        finally:
            reset_durable_run(token)

        # Past the pivot: complete-forward (manual), never compensate.
        assert ei.value.code == "saga.forward_incomplete"
        assert effects == ["do:a", "do:p", "do:c"]  # no "undo:*"

    async def test_compensation_skips_none_and_collects_raised_errors(self) -> None:
        # No-tx-route actions/compensations, a completed step with no compensation (skipped
        # on rollback), and a compensation that raises (collected, not swallowing the rest).
        ctx = context_from_modules(MockDepsModule())
        effects: list[str] = []
        saga: SagaDefinition[OrderCtx] = SagaDefinition(
            name="order",
            steps=(
                _flex_step("a", effects, tx_route=None, comp="raise"),
                _flex_step("b", effects, tx_route=None, comp="none"),
                _flex_step("c", effects, tx_route=None, fail=True),
            ),
        )

        token = _bound()
        try:
            with pytest.raises(CoreException) as ei:
                await DurableSagaExecutor().run(ctx, saga, OrderCtx())
        finally:
            reset_durable_run(token)

        # A compensation that raised makes the rollback non-clean -> INFRASTRUCTURE, and the
        # collected compensation error rides along in the failure details.
        assert ei.value.kind is ExceptionKind.INFRASTRUCTURE
        assert any(
            "a undo failed" in str(e)
            for e in (ei.value.details or {}).get("compensation_errors", [])
        )
        # Rollback is reverse over completed steps: b has no compensation (skipped), a's
        # compensation runs and raises (collected into the failure).
        assert effects == ["do:a", "do:b", "do:c", "undo:a"]

    async def test_replay_after_rollback_does_not_repeat_the_failed_step(self) -> None:
        # A saga that fails pre-pivot and rolls back must, on a second invocation under the
        # same durable run, re-raise the recorded failure WITHOUT re-running the failed
        # step's action — so a non-idempotent failed step's effect stays exactly-once.
        state = MockState()  # one journal shared across both invocations
        effects: list[str] = []
        saga: SagaDefinition[OrderCtx] = SagaDefinition(
            name="order",
            steps=(
                _step("a", effects),
                _step("b", effects),
                _step("c", effects, fail=True),
            ),
        )
        executor = DurableSagaExecutor()
        rolled_back = ["do:a", "do:b", "do:c", "undo:b", "undo:a"]

        ctx1 = context_from_modules(MockDepsModule(state=state))
        token = _bound()
        try:
            with pytest.raises(CoreException):
                await executor.run(ctx1, saga, OrderCtx())
        finally:
            reset_durable_run(token)

        assert effects == rolled_back

        # Re-invoke under the SAME run: a, b replay from the journal, c re-raises its recorded
        # failure without re-running do:c, and the compensations replay — effects unchanged.
        ctx2 = context_from_modules(MockDepsModule(state=state))
        token = _bound()
        try:
            with pytest.raises(CoreException):
                await executor.run(ctx2, saga, OrderCtx())
        finally:
            reset_durable_run(token)

        assert effects == rolled_back  # no second "do:c"

    async def test_saga_handler_requires_an_initial_context(self) -> None:
        ctx = context_from_modules(MockDepsModule())
        saga: SagaDefinition[OrderCtx] = SagaDefinition(
            name="order", steps=(_flex_step("a", []),)
        )
        handler = durable_saga_handler(saga, OrderCtx)

        with pytest.raises(CoreException, match="initial context"):
            await handler(ctx, None)
