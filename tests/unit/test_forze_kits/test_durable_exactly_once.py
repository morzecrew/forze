"""Property: a durable run applies each step effect exactly once across a crash.

Parametrized over every crash boundary (crash after 0..N journaled steps): whatever point
the process died, recovery replays the journaled steps and runs the rest, so each step's
side effect fires exactly once and the output is identical. This is the crash/recovery
correctness the durable journal exists to guarantee, checked at every interleaving point.

# covers: DurableFunctionRunner.recover
"""

from __future__ import annotations

from datetime import timedelta

import pytest

from forze.application.contracts.durable.function import (
    DurableRunContext,
    DurableRunStatus,
    bind_durable_run,
    reset_durable_run,
)
from forze.application.execution import ExecutionContext
from forze.base.primitives import utcnow
from forze_kits.integrations.durable import (
    DurableFunctionRegistry,
    DurableFunctionRunner,
    resolve_durable_run_store,
    resolve_durable_step,
)
from forze_mock import MockDepsModule, MockState
from tests.support.execution_context import context_from_modules

# ----------------------- #

_STEPS = 3


def _registry(effects: list[str]) -> DurableFunctionRegistry:
    registry = DurableFunctionRegistry()

    async def handler(ctx: ExecutionContext, input_json: dict | None) -> dict:
        step = resolve_durable_step(ctx)
        acc: list[int] = []

        for i in range(_STEPS):

            async def work(i: int = i) -> dict:
                effects.append(f"s{i}")
                return {"i": i}

            result = await step.run(f"s{i}", work)
            acc.append(result["i"])

        return {"acc": acc}

    registry.register("fn", handler)
    return registry


@pytest.mark.parametrize("crash_after", range(_STEPS + 1))
async def test_step_effects_apply_exactly_once_across_a_crash(crash_after: int) -> None:
    state = MockState()
    ctx = context_from_modules(MockDepsModule(state=state))
    effects: list[str] = []
    runner = DurableFunctionRunner(registry=_registry(effects))
    store = resolve_durable_run_store(ctx)

    record = await store.enqueue("fn", input_json=None)
    await store.begin(record.run_id, lease_for=timedelta(minutes=5))

    # Simulate a crash after `crash_after` steps: journal those steps exactly as the handler
    # would (recording their one-time effect), leaving the run RUNNING mid-flight.
    token = bind_durable_run(DurableRunContext(run_id=record.run_id, name="fn"))
    try:
        step = resolve_durable_step(ctx)
        for i in range(crash_after):

            async def work(i: int = i) -> dict:
                effects.append(f"s{i}")
                return {"i": i}

            await step.run(f"s{i}", work)
    finally:
        reset_durable_run(token)

    # Expire the crashed run's lease and recover it.
    state.durable_runs[record.run_id]["leased_until"] = utcnow() - timedelta(hours=1)
    assert await runner.recover(ctx) == 1

    reloaded = await store.load(record.run_id)
    assert reloaded is not None
    assert reloaded.status is DurableRunStatus.COMPLETED
    assert reloaded.output_json == {"acc": [0, 1, 2]}
    # Each step effect fired exactly once regardless of where the crash landed.
    assert effects == [f"s{i}" for i in range(_STEPS)]
