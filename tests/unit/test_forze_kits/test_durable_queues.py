"""Durable function queues: a recovery worker claims only the functions it selects.

# covers: DurableFunctionRegistry.functions_in
# covers: DurableFunctionRunner.recover
# covers: durable_recovery_background_lifecycle_step
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any

import pytest

from forze.application.execution import DepsRegistry, ExecutionContext, ExecutionRuntime
from forze.base.exceptions import CoreException, ExceptionKind
from forze_kits.integrations.durable import (
    DurableFunctionRegistry,
    DurableFunctionRunner,
    durable_recovery_background_lifecycle_step,
)
from forze_mock import MockDepsModule, MockState

# ----------------------- #


async def _body(ctx: ExecutionContext, input_json: dict[str, Any] | None) -> dict[str, Any]:
    return {}


def _runner() -> DurableFunctionRunner:
    registry = DurableFunctionRegistry()
    registry.register("load", _body)
    registry.register("build", _body)
    registry.register("draft", _body, queue="llm")

    return DurableFunctionRunner(registry=registry)


def _runtime(state: MockState) -> ExecutionRuntime:
    return ExecutionRuntime(deps=DepsRegistry.from_modules(MockDepsModule(state=state)).freeze())


async def _enqueue(state: MockState, *names: str) -> dict[str, str]:
    runner = _runner()
    runtime = _runtime(state)

    async with runtime.scope():
        ctx = runtime.get_context()

        return {name: (await runner.enqueue(ctx, name)).run_id for name in names}


async def _serve(state: MockState, until: list[str], **selector: Any) -> None:
    """Run one recovery worker with *selector* until the runs *until* complete."""

    step = durable_recovery_background_lifecycle_step(
        runner=_runner(), interval=timedelta(seconds=0.01), **selector
    )
    runtime = _runtime(state)

    async with runtime.scope():
        ctx = runtime.get_context()
        await step.startup(ctx)

        for _ in range(100):
            await asyncio.sleep(0.01)

            if all(state.durable_runs[run_id]["status"] == "completed" for run_id in until):
                break

        await step.shutdown(ctx)


# ....................... #


class TestRegistryQueues:
    def test_functions_resolve_by_queue_with_default_for_the_unnamed(self) -> None:
        registry = _runner().registry

        assert registry.functions_in({"default"}) == {"load", "build"}
        assert registry.functions_in({"llm"}) == {"draft"}
        assert registry.functions_in({"default", "llm"}) == {"load", "build", "draft"}

    def test_a_queue_no_function_is_in_is_refused(self) -> None:
        with pytest.raises(CoreException) as ei:
            _runner().registry.functions_in({"llm", "lmm"})

        assert ei.value.kind is ExceptionKind.CONFIGURATION
        assert ei.value.code == "durable_queue_unknown"
        assert "lmm" in ei.value.summary

    def test_a_blank_queue_is_refused(self) -> None:
        with pytest.raises(CoreException, match="queue name"):
            DurableFunctionRegistry().register("load", _body, queue=" ")


# ....................... #


class TestRecoverySelectorValidation:
    @pytest.mark.parametrize(
        "over,code",
        [
            ({"queues": {"llm"}, "functions": {"draft"}}, "durable_selector_conflict"),
            ({"queues": set()}, "durable_selector_empty"),
            ({"functions": set()}, "durable_selector_empty"),
        ],
    )
    def test_refused_when_built(self, over: dict[str, Any], code: str) -> None:
        with pytest.raises(CoreException) as ei:
            durable_recovery_background_lifecycle_step(runner=_runner(), **over)

        assert ei.value.code == code

    @pytest.mark.parametrize(
        "over,code",
        [
            ({"queues": {"default", "slow"}}, "durable_queue_unknown"),
            ({"functions": {"load", "unknown"}}, "durable_function_unregistered"),
        ],
    )
    async def test_refused_at_startup(self, over: dict[str, Any], code: str) -> None:
        step = durable_recovery_background_lifecycle_step(runner=_runner(), **over)
        runtime = _runtime(MockState())

        async with runtime.scope():
            with pytest.raises(CoreException) as ei:
                await step.startup(runtime.get_context())

        assert ei.value.code == code


# ....................... #


class TestQueueWorkers:
    async def test_each_worker_claims_only_its_queue(self) -> None:
        state = MockState()
        runs = await _enqueue(state, "load", "build", "draft")

        await _serve(state, [runs["load"], runs["build"]], queues={"default"})

        assert state.durable_runs[runs["load"]]["status"] == "completed"
        assert state.durable_runs[runs["build"]]["status"] == "completed"
        # Left to the worker serving "llm", neither claimed nor failed.
        assert state.durable_runs[runs["draft"]]["status"] == "pending"
        assert state.durable_runs[runs["draft"]]["attempts"] == 0

        await _serve(state, [runs["draft"]], queues={"llm"})

        assert state.durable_runs[runs["draft"]]["status"] == "completed"

    async def test_a_worker_selecting_functions_claims_only_those(self) -> None:
        state = MockState()
        runs = await _enqueue(state, "load", "draft")

        await _serve(state, [runs["draft"]], functions={"draft"})

        assert state.durable_runs[runs["draft"]]["status"] == "completed"
        assert state.durable_runs[runs["load"]]["status"] == "pending"
