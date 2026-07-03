"""Durable-function runner over the mock run store + step journal.

# covers: DurableFunctionRunner.run_now
# covers: DurableFunctionRunner.enqueue
# covers: DurableFunctionRunner.recover
# covers: DurableFunctionRegistry.register
# covers: DurableFunctionRegistry.get
"""

from __future__ import annotations

import pytest

from forze.application.contracts.durable.function import DurableRunStatus
from forze.application.execution import ExecutionContext
from forze.base.exceptions import CoreException, exc
from tests.support.execution_context import context_from_modules

from forze_kits.integrations.durable import (
    DurableFunctionRegistry,
    DurableFunctionRunner,
    resolve_durable_run_store,
    resolve_durable_step,
)
from forze_mock import MockDepsModule

# ----------------------- #


def _double_registry(calls: list[int]) -> DurableFunctionRegistry:
    registry = DurableFunctionRegistry()

    async def handler(ctx: ExecutionContext, input_json: dict | None) -> dict:
        step = resolve_durable_step(ctx)
        value = (input_json or {}).get("n", 0)

        async def work() -> dict:
            calls.append(1)
            return {"doubled": value * 2}

        return await step.run("double", work)

    registry.register("double", handler)
    return registry


# ....................... #


class TestDurableRunner:
    async def test_run_now_executes_and_completes(self) -> None:
        ctx = context_from_modules(MockDepsModule())
        calls: list[int] = []
        runner = DurableFunctionRunner(registry=_double_registry(calls))

        record = await runner.run_now(ctx, "double", {"n": 21})

        assert record.status is DurableRunStatus.COMPLETED
        assert record.output_json == {"doubled": 42}
        assert len(calls) == 1

    async def test_idempotency_key_dedups_to_one_execution(self) -> None:
        ctx = context_from_modules(MockDepsModule())
        calls: list[int] = []
        runner = DurableFunctionRunner(registry=_double_registry(calls))

        first = await runner.run_now(ctx, "double", {"n": 5}, idempotency_key="k1")
        second = await runner.run_now(ctx, "double", {"n": 5}, idempotency_key="k1")

        # The re-submit converges on the completed run and does not execute again.
        assert first.run_id == second.run_id
        assert second.status is DurableRunStatus.COMPLETED
        assert len(calls) == 1

    async def test_enqueue_is_pending_until_recovered(self) -> None:
        ctx = context_from_modules(MockDepsModule())
        calls: list[int] = []
        runner = DurableFunctionRunner(registry=_double_registry(calls))
        store = resolve_durable_run_store(ctx)

        record = await runner.enqueue(ctx, "double", {"n": 7})
        assert record.status is DurableRunStatus.PENDING
        assert len(calls) == 0

        claimed = await runner.recover(ctx)
        assert claimed == 1

        reloaded = await store.load(record.run_id)
        assert reloaded is not None
        assert reloaded.status is DurableRunStatus.COMPLETED
        assert reloaded.output_json == {"doubled": 14}

    async def test_failing_body_marks_failed_and_does_not_reraise_in_recovery(
        self,
    ) -> None:
        ctx = context_from_modules(MockDepsModule())
        registry = DurableFunctionRegistry()

        async def boom(ctx: ExecutionContext, input_json: dict | None) -> dict:
            raise exc.internal("boom")

        registry.register("boom", boom)
        runner = DurableFunctionRunner(registry=registry)
        store = resolve_durable_run_store(ctx)

        record = await runner.enqueue(ctx, "boom")
        claimed = await runner.recover(ctx)  # swallows the failure
        assert claimed == 1

        reloaded = await store.load(record.run_id)
        assert reloaded is not None
        assert reloaded.status is DurableRunStatus.FAILED
        assert "boom" in (reloaded.error or "")

    async def test_run_now_reraises_a_body_failure(self) -> None:
        ctx = context_from_modules(MockDepsModule())
        registry = DurableFunctionRegistry()

        async def boom(ctx: ExecutionContext, input_json: dict | None) -> dict:
            raise exc.internal("boom")

        registry.register("boom", boom)
        runner = DurableFunctionRunner(registry=registry)

        with pytest.raises(CoreException, match="boom"):
            await runner.run_now(ctx, "boom")


class TestDurableFunctionRegistry:
    async def test_duplicate_registration_rejected(self) -> None:
        registry = DurableFunctionRegistry()

        async def handler(ctx: ExecutionContext, input_json: dict | None) -> None:
            return None

        registry.register("a", handler)

        with pytest.raises(CoreException, match="already registered"):
            registry.register("a", handler)

    async def test_missing_registration_raises(self) -> None:
        with pytest.raises(CoreException, match="No durable function"):
            DurableFunctionRegistry().get("nope")
