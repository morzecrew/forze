"""In-process saga executor: happy path, reverse compensation, guard, retry."""

from __future__ import annotations

import asyncio

import pytest

from forze.application.contracts.saga import SagaDefinition, SagaStep, SagaStepKind
from forze.application.contracts.transaction import COMMIT_AMBIGUOUS_CODE
from forze.application.execution import ExecutionContext, run_saga
from forze.base.exceptions import CoreException, ExceptionKind, exc
from forze_mock import MockDepsModule
from tests.support.execution_context import context_from_modules

# ----------------------- #

State = list[str]


def _step(
    name: str,
    recorder: list[str],
    *,
    fail: bool = False,
    comp: bool = True,
    comp_fail: bool = False,
    kind: SagaStepKind = SagaStepKind.COMPENSATABLE,
    tx_route: str | None = "mock",
) -> SagaStep[State]:
    async def action(_ctx: ExecutionContext, state: State) -> State:
        recorder.append(f"do:{name}")
        if fail:
            raise exc.infrastructure(f"{name} failed")
        return [*state, name]

    async def compensation(_ctx: ExecutionContext, _state: State) -> None:
        recorder.append(f"undo:{name}")
        if comp_fail:
            raise exc.internal(f"{name} compensation failed")

    return SagaStep(
        name=name,
        action=action,
        compensation=compensation if comp else None,
        kind=kind,
        tx_route=tx_route,
        idempotent=kind is SagaStepKind.RETRYABLE,  # retryable steps must affirm this
    )


class TestSagaExecutor:
    async def test_happy_path_runs_all_steps_and_threads_context(self) -> None:
        rec: list[str] = []
        ctx = context_from_modules(MockDepsModule())
        saga: SagaDefinition[State] = SagaDefinition(
            name="s", steps=(_step("a", rec), _step("b", rec), _step("c", rec))
        )

        result = await run_saga(ctx, saga, [])

        assert result == ["a", "b", "c"]
        assert rec == ["do:a", "do:b", "do:c"]

    async def test_failing_step_compensates_completed_in_reverse(self) -> None:
        rec: list[str] = []
        ctx = context_from_modules(MockDepsModule())
        saga: SagaDefinition[State] = SagaDefinition(
            name="s",
            steps=(_step("a", rec), _step("b", rec), _step("c", rec, fail=True)),
        )

        with pytest.raises(CoreException) as ei:
            await run_saga(ctx, saga, [])

        assert ei.value.kind is ExceptionKind.DOMAIN  # consistent: compensated
        # c failed (not completed -> not compensated); a, b compensated in reverse.
        assert rec == ["do:a", "do:b", "do:c", "undo:b", "undo:a"]

    async def test_step_without_compensation_is_skipped_on_rollback(self) -> None:
        rec: list[str] = []
        ctx = context_from_modules(MockDepsModule())
        saga: SagaDefinition[State] = SagaDefinition(
            name="s",
            steps=(_step("a", rec, comp=False), _step("b", rec, fail=True)),
        )

        with pytest.raises(CoreException):
            await run_saga(ctx, saga, [])

        assert rec == ["do:a", "do:b"]  # a has no compensation -> nothing to undo

    async def test_compensation_failure_surfaces_distinct_error(self) -> None:
        rec: list[str] = []
        ctx = context_from_modules(MockDepsModule())
        saga: SagaDefinition[State] = SagaDefinition(
            name="s",
            steps=(_step("a", rec, comp_fail=True), _step("b", rec, fail=True)),
        )

        with pytest.raises(CoreException) as ei:
            await run_saga(ctx, saga, [])

        # inconsistent: compensation itself failed -> infrastructure, not domain.
        assert ei.value.kind is ExceptionKind.INFRASTRUCTURE

    async def test_running_inside_a_transaction_raises(self) -> None:
        ctx = context_from_modules(MockDepsModule())
        saga: SagaDefinition[State] = SagaDefinition(name="s", steps=(_step("a", []),))

        with pytest.raises(CoreException) as ei:
            async with ctx.tx_ctx.scope("mock"):
                await run_saga(ctx, saga, [])

        assert ei.value.kind is ExceptionKind.CONFIGURATION

    async def test_retry_policy_retries_a_transient_step(self) -> None:
        rec: list[str] = []
        attempts = {"n": 0}
        # The mock registers a passthrough resilience executor by default; opt into the
        # real one so the per-step retry_policy actually retries.
        ctx = context_from_modules(MockDepsModule(resilience="real"))

        async def flaky(_ctx: ExecutionContext, state: State) -> State:
            attempts["n"] += 1
            rec.append("do:flaky")
            if attempts["n"] == 1:
                raise exc.concurrency("transient conflict")
            return [*state, "flaky"]

        saga: SagaDefinition[State] = SagaDefinition(
            name="s",
            steps=(
                SagaStep(
                    name="flaky",
                    action=flaky,
                    tx_route="mock",
                    retry_policy="occ",
                    idempotent=True,
                ),
            ),
        )

        result = await run_saga(ctx, saga, [])

        assert result == ["flaky"]
        assert attempts["n"] == 2  # retried once via the occ policy


class TestAmbiguousCommit:
    """An interruption at a step's own commit is indeterminacy, not failure."""

    @staticmethod
    def _ambiguous_step(
        name: str,
        rec: list[str],
        *,
        kind: SagaStepKind = SagaStepKind.COMPENSATABLE,
    ) -> SagaStep[State]:
        async def action(_ctx: ExecutionContext, state: State) -> State:
            rec.append(f"do:{name}")
            # The exact error the tx scope raises when a drain-timeout cancel lands
            # at the commit: the step MAY be committed.
            raise exc.internal(
                "Cancelled at or after the transaction commit",
                code=COMMIT_AMBIGUOUS_CODE,
            )

        return SagaStep(
            name=name,
            action=action,
            compensation=None,
            kind=kind,
            tx_route="mock",
            idempotent=kind is SagaStepKind.RETRYABLE,
        )

    async def test_ambiguous_step_commit_compensates_nothing(self) -> None:
        # If B may be committed, compensating A would roll the saga back *around* a
        # live effect (split-brain), and DOMAIN ``saga.step_failed`` would falsely
        # certify "completed steps were compensated". Nothing is compensated and the
        # raised error names the indeterminacy for the operator.
        rec: list[str] = []
        ctx = context_from_modules(MockDepsModule())
        saga: SagaDefinition[State] = SagaDefinition(
            name="s",
            steps=(_step("a", rec), self._ambiguous_step("b", rec)),
        )

        with pytest.raises(CoreException) as ei:
            await run_saga(ctx, saga, [])

        assert ei.value.code == "saga.step_ambiguous"
        assert ei.value.kind is ExceptionKind.INFRASTRUCTURE  # never a "consistent" DOMAIN
        assert rec == ["do:a", "do:b"]  # a's compensation never ran

    async def test_ambiguous_commit_after_pivot_still_reports_indeterminacy(self) -> None:
        # Past the pivot the failure path is forward-incomplete; ambiguity still wins:
        # "could not complete forward" presumes the step failed, which is unknown here.
        rec: list[str] = []
        ctx = context_from_modules(MockDepsModule())
        saga: SagaDefinition[State] = SagaDefinition(
            name="s",
            steps=(
                _step("a", rec, kind=SagaStepKind.PIVOT),
                self._ambiguous_step("b", rec, kind=SagaStepKind.RETRYABLE),
            ),
        )

        with pytest.raises(CoreException) as ei:
            await run_saga(ctx, saga, [])

        assert ei.value.code == "saga.step_ambiguous"
        assert rec == ["do:a", "do:b"]

    async def test_raw_cancel_at_commit_outside_an_operation_reports_indeterminacy(
        self,
    ) -> None:
        # Outside an operation-owned task the tx scope re-raises a cancellation RAW
        # (its commit_ambiguous conversion is operation-only), bypassing ``except
        # Exception``: the caller would get a plain cancellation it could retry into
        # a duplicate of the possibly-committed step. The executor must classify it.
        from forze.application.execution.context.commit_state import mark_commit_started

        rec: list[str] = []
        ctx = context_from_modules(MockDepsModule())

        async def cancelled_at_commit(_ctx: ExecutionContext, _state: State) -> State:
            rec.append("do:b")
            # The tx scope sets this mark right before the driver commit runs; the
            # cancel then lands inside that commit and reaches us raw (no operation).
            mark_commit_started()
            raise asyncio.CancelledError

        saga: SagaDefinition[State] = SagaDefinition(
            name="s",
            steps=(
                _step("a", rec),
                SagaStep(name="b", action=cancelled_at_commit, compensation=None, tx_route="mock"),
            ),
        )

        with pytest.raises(CoreException) as ei:
            await run_saga(ctx, saga, [])

        assert ei.value.code == "saga.step_ambiguous"
        assert rec == ["do:a", "do:b"]  # a's compensation never ran

    async def test_raw_cancel_before_commit_stays_a_cancellation(self) -> None:
        # A cancel in the step BODY rolled back cleanly: crash-shaped, propagated as a
        # cancellation. Step a's committed transaction leaves the task's commit mark
        # set, so this also pins the per-step reset — without it, b's pre-commit
        # cancel would misread as ambiguous.
        rec: list[str] = []
        ctx = context_from_modules(MockDepsModule())

        async def cancelled_in_body(_ctx: ExecutionContext, _state: State) -> State:
            rec.append("do:b")
            raise asyncio.CancelledError

        saga: SagaDefinition[State] = SagaDefinition(
            name="s",
            steps=(
                _step("a", rec),
                SagaStep(name="b", action=cancelled_in_body, compensation=None, tx_route="mock"),
            ),
        )

        with pytest.raises(asyncio.CancelledError):
            await run_saga(ctx, saga, [])

        assert rec == ["do:a", "do:b"]  # unwound crash-shaped: no compensation, no wrap

    async def test_inside_an_operation_the_mark_is_left_to_the_boundary(self) -> None:
        # Operation-owned tasks rely on the tx scope's own conversion (exercised above
        # via the coded CoreException). The executor must not touch the commit mark
        # there — the invocation boundary reads it to classify a deadline that tore a
        # commit — and a raw cancel passes through untouched for the scope/boundary
        # pair to handle.
        from forze.application.execution.context.active_operation import (
            active_operation_var,
        )
        from forze.application.execution.context.commit_state import (
            commit_started,
            mark_commit_started,
            reset_commit_started,
        )

        rec: list[str] = []
        ctx = context_from_modules(MockDepsModule())

        async def cancelled_at_commit(_ctx: ExecutionContext, _state: State) -> State:
            rec.append("do:b")
            mark_commit_started()
            raise asyncio.CancelledError

        saga: SagaDefinition[State] = SagaDefinition(
            name="s",
            steps=(
                SagaStep(name="b", action=cancelled_at_commit, compensation=None, tx_route="mock"),
            ),
        )

        task = asyncio.current_task()
        assert task is not None
        token = active_operation_var.set(task)
        reset_commit_started()

        try:
            with pytest.raises(asyncio.CancelledError):
                await run_saga(ctx, saga, [])

            # The mark set at the torn commit survives for the boundary to read.
            assert commit_started()

        finally:
            active_operation_var.reset(token)
            reset_commit_started()


class TestPivotSemantics:
    async def test_failure_after_pivot_does_not_compensate(self) -> None:
        rec: list[str] = []
        ctx = context_from_modules(MockDepsModule())
        saga: SagaDefinition[State] = SagaDefinition(
            name="s",
            steps=(
                _step("a", rec),
                _step("p", rec, kind=SagaStepKind.PIVOT),
                _step("r", rec, kind=SagaStepKind.RETRYABLE, fail=True),
            ),
        )

        with pytest.raises(CoreException) as ei:
            await run_saga(ctx, saga, [])

        # committed at the pivot -> forward-incomplete, NOT compensated.
        assert ei.value.kind is ExceptionKind.INFRASTRUCTURE
        assert "forward_incomplete" in (ei.value.code or "")
        assert rec == ["do:a", "do:p", "do:r"]  # no undo:* — nothing compensated

    async def test_pivot_failure_compensates_prior_steps(self) -> None:
        rec: list[str] = []
        ctx = context_from_modules(MockDepsModule())
        saga: SagaDefinition[State] = SagaDefinition(
            name="s",
            steps=(
                _step("a", rec),
                _step("p", rec, kind=SagaStepKind.PIVOT, fail=True),
            ),
        )

        with pytest.raises(CoreException) as ei:
            await run_saga(ctx, saga, [])

        # pivot failed before committing -> compensate the prior compensatable step.
        assert ei.value.kind is ExceptionKind.DOMAIN
        assert rec == ["do:a", "do:p", "undo:a"]


class TestCompensationRetry:
    async def test_compensation_is_retried_under_its_policy(self) -> None:
        rec: list[str] = []
        comp_attempts = {"n": 0}
        ctx = context_from_modules(MockDepsModule(resilience="real"))

        async def action(_ctx: ExecutionContext, state: State) -> State:
            rec.append("do:a")
            return [*state, "a"]

        async def compensation(_ctx: ExecutionContext, _state: State) -> None:
            comp_attempts["n"] += 1
            rec.append("undo:a")
            if comp_attempts["n"] == 1:
                raise exc.concurrency("transient compensation conflict")

        a = SagaStep(
            name="a",
            action=action,
            compensation=compensation,
            tx_route="mock",
            compensation_policy="occ",
        )
        saga: SagaDefinition[State] = SagaDefinition(
            name="s", steps=(a, _step("b", rec, fail=True))
        )

        with pytest.raises(CoreException) as ei:
            await run_saga(ctx, saga, [])

        # compensation retried to success -> consistent (DOMAIN, not compensation_failed).
        assert ei.value.kind is ExceptionKind.DOMAIN
        assert comp_attempts["n"] == 2


class TestDefinitionValidation:
    def test_retryable_without_pivot_rejected(self) -> None:
        rec: list[str] = []
        with pytest.raises(CoreException) as ei:
            SagaDefinition(
                name="s", steps=(_step("r", rec, kind=SagaStepKind.RETRYABLE),)
            )
        assert ei.value.kind is ExceptionKind.CONFIGURATION

    def test_compensatable_after_pivot_rejected(self) -> None:
        rec: list[str] = []
        with pytest.raises(CoreException):
            SagaDefinition(
                name="s",
                steps=(_step("p", rec, kind=SagaStepKind.PIVOT), _step("a", rec)),
            )

    def test_two_pivots_rejected(self) -> None:
        rec: list[str] = []
        with pytest.raises(CoreException):
            SagaDefinition(
                name="s",
                steps=(
                    _step("p1", rec, kind=SagaStepKind.PIVOT),
                    _step("p2", rec, kind=SagaStepKind.PIVOT),
                ),
            )

    def test_retried_step_must_declare_idempotent(self) -> None:
        async def act(_ctx: ExecutionContext, state: State) -> State:
            return state

        # A retry_policy step without idempotent=True is rejected.
        with pytest.raises(CoreException) as ei:
            SagaDefinition(
                name="s",
                steps=(
                    SagaStep(name="a", action=act, retry_policy="occ", tx_route="mock"),
                ),
            )
        assert ei.value.kind is ExceptionKind.CONFIGURATION

        # Same step with idempotent=True is accepted.
        SagaDefinition(
            name="s",
            steps=(
                SagaStep(
                    name="a",
                    action=act,
                    retry_policy="occ",
                    tx_route="mock",
                    idempotent=True,
                ),
            ),
        )
