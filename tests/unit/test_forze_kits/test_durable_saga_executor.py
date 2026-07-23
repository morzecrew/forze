"""Durable saga executor: journaled steps, reverse compensation, crash resume, guards.

# covers: DurableSagaExecutor.run
# covers: durable_saga_handler
"""

from __future__ import annotations

import asyncio
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
from forze_kits.integrations.durable import (
    DurableFunctionRegistry,
    DurableFunctionRunner,
    DurableSagaExecutor,
    durable_saga_handler,
    resolve_durable_run_store,
    resolve_durable_step,
)
from forze_mock import MockDepsModule, MockState
from tests.support.execution_context import context_from_modules

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
            raise exc.precondition(f"{name} failed")
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

    async def test_ambiguous_step_commit_survives_the_journal_and_replay(self) -> None:
        # A drain-timeout cancel at a step's commit journals like any failure — but the
        # journal must keep the ``commit_ambiguous`` code: flattened to a message, the
        # replayed failure would be compensated around a possibly-committed effect and
        # recorded as DOMAIN "compensated, consistent", permanently. Live pass and
        # replay must both refuse to compensate and raise ``saga.step_ambiguous``.
        from forze.application.contracts.transaction import COMMIT_AMBIGUOUS_CODE

        state = MockState()
        effects: list[str] = []

        def _ambiguous(name: str) -> SagaStep[OrderCtx]:
            async def action(_ctx: ExecutionContext, _state: OrderCtx) -> OrderCtx:
                effects.append(f"do:{name}")
                raise exc.internal(
                    "Cancelled at or after the transaction commit",
                    code=COMMIT_AMBIGUOUS_CODE,
                )

            return SagaStep(name=name, action=action, compensation=None, tx_route="mock")

        saga: SagaDefinition[OrderCtx] = SagaDefinition(
            name="order",
            steps=(_step("a", effects), _ambiguous("b")),
        )
        executor = DurableSagaExecutor()

        ctx1 = context_from_modules(MockDepsModule(state=state))
        token = _bound()
        try:
            with pytest.raises(CoreException) as live:
                await executor.run(ctx1, saga, OrderCtx())
        finally:
            reset_durable_run(token)

        assert live.value.code == "saga.step_ambiguous"
        assert live.value.kind is ExceptionKind.INFRASTRUCTURE
        assert effects == ["do:a", "do:b"]  # a's compensation never ran

        # Replay under the same run: the failed body is not re-run, and the journaled
        # outcome keeps its classification instead of degrading to step_failed.
        ctx2 = context_from_modules(MockDepsModule(state=state))
        token = _bound()
        try:
            with pytest.raises(CoreException) as replayed:
                await executor.run(ctx2, saga, OrderCtx())
        finally:
            reset_durable_run(token)

        assert replayed.value.code == "saga.step_ambiguous"
        assert replayed.value.kind is ExceptionKind.INFRASTRUCTURE
        assert effects == ["do:a", "do:b"]  # nothing re-ran, nothing compensated

    async def test_cancel_at_step_commit_journals_ambiguity_not_a_rerun(self) -> None:
        # The runner's task is not operation-owned, so a drain cancel at a step's
        # commit reaches the executor as a RAW CancelledError (the tx scope's
        # commit_ambiguous conversion is operation-only) — bypassing ``except
        # Exception``. Unjournaled, replay would re-run the possibly-committed body
        # (duplicate effects). The executor must journal the ambiguity itself.
        from forze.application.execution.context.commit_state import mark_commit_started

        state = MockState()
        effects: list[str] = []

        def _cancelled_at_commit(name: str) -> SagaStep[OrderCtx]:
            async def action(_ctx: ExecutionContext, _state: OrderCtx) -> OrderCtx:
                effects.append(f"do:{name}")
                # The tx scope sets this mark right before the driver commit runs;
                # the cancel then lands inside that commit.
                mark_commit_started()
                raise asyncio.CancelledError

            return SagaStep(name=name, action=action, compensation=None, tx_route="mock")

        saga: SagaDefinition[OrderCtx] = SagaDefinition(
            name="order",
            steps=(_step("a", effects), _cancelled_at_commit("b")),
        )
        executor = DurableSagaExecutor()

        ctx1 = context_from_modules(MockDepsModule(state=state))
        token = _bound()
        try:
            with pytest.raises(CoreException) as live:
                await executor.run(ctx1, saga, OrderCtx())
        finally:
            reset_durable_run(token)

        assert live.value.code == "saga.step_ambiguous"
        assert effects == ["do:a", "do:b"]  # no compensation around the unknown outcome

        # Replay: the journaled ambiguity is re-raised; the body is NOT re-run.
        ctx2 = context_from_modules(MockDepsModule(state=state))
        token = _bound()
        try:
            with pytest.raises(CoreException) as replayed:
                await executor.run(ctx2, saga, OrderCtx())
        finally:
            reset_durable_run(token)

        assert replayed.value.code == "saga.step_ambiguous"
        assert effects == ["do:a", "do:b"]  # unchanged: no duplicate effect on replay

    async def test_cancel_before_step_commit_stays_crash_shaped_and_resumable(self) -> None:
        # A cancel landing in the step BODY rolled back cleanly: nothing committed, so
        # it must propagate as a cancellation (no journal row, no false ambiguity) and
        # the run must resume by re-running the body — the durable plane's crash
        # contract. Step a's own committed transaction leaves the task's commit mark
        # set, so this also pins the per-step mark reset: without it, b's pre-commit
        # cancel would misread as ambiguous.
        state = MockState()
        effects: list[str] = []
        cancelled_once = False

        def _cancelled_in_body(name: str) -> SagaStep[OrderCtx]:
            async def action(_ctx: ExecutionContext, state_: OrderCtx) -> OrderCtx:
                nonlocal cancelled_once
                effects.append(f"do:{name}")

                if not cancelled_once:
                    cancelled_once = True
                    raise asyncio.CancelledError

                return OrderCtx(trail=[*state_.trail, name])

            return SagaStep(name=name, action=action, compensation=None, tx_route="mock")

        saga: SagaDefinition[OrderCtx] = SagaDefinition(
            name="order",
            steps=(_step("a", effects), _cancelled_in_body("b")),
        )
        executor = DurableSagaExecutor()

        ctx1 = context_from_modules(MockDepsModule(state=state))
        token = _bound()
        try:
            with pytest.raises(asyncio.CancelledError):
                await executor.run(ctx1, saga, OrderCtx())
        finally:
            reset_durable_run(token)

        assert effects == ["do:a", "do:b"]

        # Resume: a replays from the journal, b's body re-runs and completes.
        ctx2 = context_from_modules(MockDepsModule(state=state))
        token = _bound()
        try:
            result = await executor.run(ctx2, saga, OrderCtx())
        finally:
            reset_durable_run(token)

        assert result.trail == ["a", "b"]
        assert effects == ["do:a", "do:b", "do:b"]  # only b re-ran; a came from the journal

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
            raise exc.precondition(f"{name} failed")
        return OrderCtx(trail=[*state.trail, name])

    async def compensation(_ctx: ExecutionContext, _state: OrderCtx) -> None:
        effects.append(f"undo:{name}")
        if comp == "raise":
            raise exc.precondition(f"{name} undo failed")

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


# ....................... #


def _flaky_step(
    name: str,
    effects: list[str],
    *,
    fail_times: int = 0,
    comp_fail_times: int = 0,
) -> SagaStep[OrderCtx]:
    """A step whose action (compensation) raises a retryable infrastructure error on its
    first *fail_times* (*comp_fail_times*) invocations, then succeeds."""

    action_blips = {"left": fail_times}
    comp_blips = {"left": comp_fail_times}

    async def action(_ctx: ExecutionContext, state: OrderCtx) -> OrderCtx:
        effects.append(f"do:{name}")
        if action_blips["left"] > 0:
            action_blips["left"] -= 1
            raise exc.infrastructure(f"{name} blipped")
        return OrderCtx(trail=[*state.trail, name])

    async def compensation(_ctx: ExecutionContext, _state: OrderCtx) -> None:
        effects.append(f"undo:{name}")
        if comp_blips["left"] > 0:
            comp_blips["left"] -= 1
            raise exc.infrastructure(f"{name} undo blipped")

    return SagaStep(
        name=name,
        action=action,
        compensation=compensation,
        tx_route="mock",
    )


class TestDurableSagaTransientFailures:
    async def test_transient_step_blip_is_retried_in_place_not_compensated(self) -> None:
        # A one-off retryable failure must be absorbed by an in-place retry of the failing
        # step alone — no compensation, no re-execution of the completed steps.
        ctx = context_from_modules(MockDepsModule())
        effects: list[str] = []
        saga: SagaDefinition[OrderCtx] = SagaDefinition(
            name="order",
            steps=(
                _step("a", effects),
                _step("b", effects),
                _flaky_step("c", effects, fail_times=1),
            ),
        )

        token = _bound()
        try:
            result = await DurableSagaExecutor(retry_base_delay=0.0).run(
                ctx, saga, OrderCtx()
            )
        finally:
            reset_durable_run(token)

        assert result.trail == ["a", "b", "c"]
        # a and b ran exactly once; only c re-ran; nothing was undone.
        assert effects == ["do:a", "do:b", "do:c", "do:c"]

    async def test_reinvocation_after_an_absorbed_blip_replays_from_the_journal(
        self,
    ) -> None:
        # An in-place retry must leave the journal coherent: a later re-invocation under
        # the same run replays every step from the journal without new side effects.
        state = MockState()
        effects: list[str] = []
        saga: SagaDefinition[OrderCtx] = SagaDefinition(
            name="order",
            steps=(_step("a", effects), _flaky_step("b", effects, fail_times=1)),
        )
        executor = DurableSagaExecutor(retry_base_delay=0.0)

        ctx1 = context_from_modules(MockDepsModule(state=state))
        token = _bound()
        try:
            first = await executor.run(ctx1, saga, OrderCtx())
        finally:
            reset_durable_run(token)

        assert first.trail == ["a", "b"]
        assert effects == ["do:a", "do:b", "do:b"]

        ctx2 = context_from_modules(MockDepsModule(state=state))
        token = _bound()
        try:
            replayed = await executor.run(ctx2, saga, OrderCtx())
        finally:
            reset_durable_run(token)

        assert replayed.trail == ["a", "b"]
        assert effects == ["do:a", "do:b", "do:b"]  # unchanged — replayed, not re-run

    async def test_exhausted_retries_journal_the_failure_and_compensate(self) -> None:
        # A retryable failure that outlives the bounded retries is a genuine failure:
        # journaled and compensated, exactly like a non-retryable one.
        ctx = context_from_modules(MockDepsModule())
        effects: list[str] = []
        saga: SagaDefinition[OrderCtx] = SagaDefinition(
            name="order",
            steps=(
                _step("a", effects),
                _step("b", effects),
                _flaky_step("c", effects, fail_times=100),
            ),
        )
        executor = DurableSagaExecutor(retry_attempts=1, retry_base_delay=0.0)

        token = _bound()
        try:
            with pytest.raises(CoreException) as ei:
                await executor.run(ctx, saga, OrderCtx())
        finally:
            reset_durable_run(token)

        assert ei.value.kind is ExceptionKind.DOMAIN  # consistent: compensated
        # c attempted twice (initial + one retry), then rolled back in reverse.
        assert effects == ["do:a", "do:b", "do:c", "do:c", "undo:b", "undo:a"]

    async def test_non_retryable_step_failure_compensates_without_retrying(self) -> None:
        ctx = context_from_modules(MockDepsModule())
        effects: list[str] = []
        saga: SagaDefinition[OrderCtx] = SagaDefinition(
            name="order",
            steps=(
                _step("a", effects),
                _step("b", effects),
                _step("c", effects, fail=True),  # precondition — non-retryable
            ),
        )

        token = _bound()
        try:
            with pytest.raises(CoreException) as ei:
                await DurableSagaExecutor(retry_base_delay=0.0).run(
                    ctx, saga, OrderCtx()
                )
        finally:
            reset_durable_run(token)

        assert ei.value.kind is ExceptionKind.DOMAIN  # consistent: compensated
        # Exactly one do:c — a non-retryable failure is never retried in place.
        assert effects == ["do:a", "do:b", "do:c", "undo:b", "undo:a"]

    async def test_transient_compensation_blip_is_retried_not_collected(self) -> None:
        # A one-off retryable failure while compensating must be absorbed the same way,
        # keeping the rollback clean instead of leaving it "compensation failed".
        ctx = context_from_modules(MockDepsModule())
        effects: list[str] = []
        saga: SagaDefinition[OrderCtx] = SagaDefinition(
            name="order",
            steps=(
                _flaky_step("a", effects, comp_fail_times=1),
                _step("b", effects, fail=True),
            ),
        )

        token = _bound()
        try:
            with pytest.raises(CoreException) as ei:
                await DurableSagaExecutor(retry_base_delay=0.0).run(
                    ctx, saga, OrderCtx()
                )
        finally:
            reset_durable_run(token)

        # The rollback ended clean (the blip was retried away), so the saga's outcome is
        # the modeled "rolled back" DOMAIN failure with no collected compensation errors.
        assert ei.value.kind is ExceptionKind.DOMAIN
        assert "compensation_errors" not in (ei.value.details or {})
        assert effects == ["do:a", "do:b", "undo:a", "undo:a"]

    async def test_blip_absorbed_saga_completes_end_to_end_through_the_runner(
        self,
    ) -> None:
        # Through the durable-function runner: a transient step blip never surfaces as a
        # terminal run failure — the run completes.
        state = MockState()
        effects: list[str] = []
        saga: SagaDefinition[OrderCtx] = SagaDefinition(
            name="order",
            steps=(_step("a", effects), _flaky_step("b", effects, fail_times=1)),
        )
        registry = DurableFunctionRegistry()
        registry.register(
            "order",
            durable_saga_handler(
                saga, OrderCtx, executor=DurableSagaExecutor(retry_base_delay=0.0)
            ),
        )
        runner = DurableFunctionRunner(registry=registry)

        ctx = context_from_modules(MockDepsModule(state=state))
        record = await runner.run_now(
            ctx, "order", OrderCtx().model_dump(mode="json")
        )

        assert record.status is DurableRunStatus.COMPLETED
        assert record.output_json == {"trail": ["a", "b"]}
        assert effects == ["do:a", "do:b", "do:b"]


class TestRetryKnobValidation:
    def test_negative_retry_attempts_rejected(self) -> None:
        with pytest.raises(CoreException) as ei:
            DurableSagaExecutor(retry_attempts=-1)
        assert ei.value.kind is ExceptionKind.CONFIGURATION

    def test_negative_retry_base_delay_rejected(self) -> None:
        with pytest.raises(CoreException) as ei:
            DurableSagaExecutor(retry_base_delay=-0.1)
        assert ei.value.kind is ExceptionKind.CONFIGURATION

    def test_zero_values_are_legitimate(self) -> None:
        # 0 attempts disables in-place retries; 0 delay retries immediately.
        executor = DurableSagaExecutor(retry_attempts=0, retry_base_delay=0.0)
        assert executor.retry_attempts == 0
