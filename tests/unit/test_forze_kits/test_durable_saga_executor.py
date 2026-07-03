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
