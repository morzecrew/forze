"""In-process saga executor: happy path, reverse compensation, guard, retry."""

from __future__ import annotations

import pytest

from forze.application.contracts.saga import SagaDefinition, SagaStep, SagaStepKind
from forze.application.execution import ExecutionContext, run_saga
from forze.base.exceptions import CoreException, ExceptionKind, exc
from tests.support.execution_context import context_from_modules

from forze_mock import MockDepsModule

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
                    name="flaky", action=flaky, tx_route="mock", retry_policy="occ"
                ),
            ),
        )

        result = await run_saga(ctx, saga, [])

        assert result == ["flaky"]
        assert attempts["n"] == 2  # retried once via the occ policy


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
