"""Crash recovery for durable runs: reclaim an abandoned run and replay completed steps.

# covers: DurableFunctionRunner.recover
# covers: durable_recovery_background_lifecycle_step
"""

from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest

from forze.application.contracts.durable.function import (
    DurableRunContext,
    DurableRunStatus,
    bind_durable_run,
    reset_durable_run,
)
from forze.application.execution import DepsRegistry, ExecutionContext, ExecutionRuntime
from forze.base.exceptions import CoreException
from forze.base.primitives import utcnow
from tests.support.execution_context import context_from_modules

from forze_kits.integrations.durable import (
    DurableFunctionRegistry,
    DurableFunctionRunner,
    durable_recovery_background_lifecycle_step,
    resolve_durable_run_store,
    resolve_durable_step,
)
from forze_mock import MockDepsModule, MockState

# ----------------------- #


def _two_step_registry(counters: dict[str, int]) -> DurableFunctionRegistry:
    registry = DurableFunctionRegistry()

    async def handler(ctx: ExecutionContext, input_json: dict | None) -> dict:
        step = resolve_durable_step(ctx)

        async def w1() -> dict:
            counters["s1"] += 1
            return {"a": 1}

        async def w2() -> dict:
            counters["s2"] += 1
            return {"b": 2}

        r1 = await step.run("s1", w1)
        r2 = await step.run("s2", w2)
        return {"r1": r1, "r2": r2}

    registry.register("fn", handler)
    return registry


# ....................... #


class TestCrashRecovery:
    async def test_crash_after_first_step_replays_it_on_recovery(self) -> None:
        state = MockState()
        ctx = context_from_modules(MockDepsModule(state=state))
        counters = {"s1": 0, "s2": 0}
        runner = DurableFunctionRunner(registry=_two_step_registry(counters))
        store = resolve_durable_run_store(ctx)

        # Enqueue + claim (RUNNING), then simulate a crash after the FIRST step: journal s1
        # under the run binding and stop without completing the run.
        record = await store.enqueue("fn", input_json=None)
        assert await store.begin(record.run_id, lease_for=timedelta(minutes=5))

        token = bind_durable_run(DurableRunContext(run_id=record.run_id, name="fn"))
        try:
            step = resolve_durable_step(ctx)

            async def w1() -> dict:
                counters["s1"] += 1
                return {"a": 1}

            await step.run("s1", w1)
        finally:
            reset_durable_run(token)

        assert counters == {"s1": 1, "s2": 0}  # only s1 ran, run left RUNNING (crashed)

        # Expire the crashed run's lease so the scanner reclaims it.
        state.durable_runs[record.run_id]["leased_until"] = utcnow() - timedelta(hours=1)

        assert await runner.recover(ctx) == 1

        reloaded = await store.load(record.run_id)
        assert reloaded is not None
        assert reloaded.status is DurableRunStatus.COMPLETED
        # s1 replayed from the journal (NOT re-run); s2 ran live to completion.
        assert counters == {"s1": 1, "s2": 1}
        assert reloaded.output_json == {"r1": {"a": 1}, "r2": {"b": 2}}

    async def test_recovery_lifecycle_step_drains_a_pending_run(self) -> None:
        state = MockState()
        counters = {"s1": 0, "s2": 0}
        runner = DurableFunctionRunner(registry=_two_step_registry(counters))
        lifecycle_step = durable_recovery_background_lifecycle_step(
            runner=runner,
            interval=timedelta(seconds=0.01),
            limit=5,
        )
        runtime = ExecutionRuntime(
            deps=DepsRegistry.from_modules(MockDepsModule(state=state)).freeze()
        )

        async with runtime.scope():
            ctx = runtime.get_context()
            record = await runner.enqueue(ctx, "fn")
            assert record.status is DurableRunStatus.PENDING

            await lifecycle_step.startup(ctx)

            reloaded = None
            for _ in range(50):
                await asyncio.sleep(0.01)
                reloaded = await resolve_durable_run_store(ctx).load(record.run_id)
                if reloaded is not None and reloaded.status is DurableRunStatus.COMPLETED:
                    break

            await lifecycle_step.shutdown(ctx)

        assert reloaded is not None
        assert reloaded.status is DurableRunStatus.COMPLETED
        assert counters == {"s1": 1, "s2": 1}


class TestRecoveryLifecycleValidation:
    def test_non_positive_interval_rejected(self) -> None:
        with pytest.raises(CoreException, match="Interval"):
            durable_recovery_background_lifecycle_step(
                runner=DurableFunctionRunner(registry=DurableFunctionRegistry()),
                interval=timedelta(seconds=0),
            )
