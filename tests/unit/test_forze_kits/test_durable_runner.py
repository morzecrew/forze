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
from forze_kits.integrations.durable import (
    DurableFunctionRegistry,
    DurableFunctionRunner,
    resolve_durable_run_store,
    resolve_durable_step,
)
from forze_mock import MockDepsModule, MockDurableRunStore
from tests.support.execution_context import context_from_modules

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

    async def test_unregistered_name_does_not_strand_the_recovery_batch(self) -> None:
        # A run whose name is no longer registered (deploy skew, a renamed function,
        # a stale schedule) is enqueued FIRST, so the sweep claims it ahead of the
        # valid runs. It must fail in place — not escape and strand the co-claimed
        # runs as leased RUNNING.
        ctx = context_from_modules(MockDepsModule())
        calls: list[int] = []
        runner = DurableFunctionRunner(registry=_double_registry(calls))
        store = resolve_durable_run_store(ctx)

        poison = await runner.enqueue(ctx, "renamed_away", {"n": 1})
        valid_one = await runner.enqueue(ctx, "double", {"n": 2})
        valid_two = await runner.enqueue(ctx, "double", {"n": 3})

        claimed = await runner.recover(ctx)
        assert claimed == 3

        poisoned = await store.load(poison.run_id)
        assert poisoned is not None
        assert poisoned.status is DurableRunStatus.FAILED
        assert "No durable function" in (poisoned.error or "")

        for run_id, doubled in ((valid_one.run_id, 4), (valid_two.run_id, 6)):
            reloaded = await store.load(run_id)
            assert reloaded is not None
            assert reloaded.status is DurableRunStatus.COMPLETED
            assert reloaded.output_json == {"doubled": doubled}

        # The failed run is terminal: a second sweep finds nothing to re-claim.
        assert await runner.recover(ctx) == 0

    async def test_unregistered_name_does_not_strand_a_concurrent_batch(self) -> None:
        ctx = context_from_modules(MockDepsModule())
        calls: list[int] = []
        runner = DurableFunctionRunner(registry=_double_registry(calls))
        store = resolve_durable_run_store(ctx)

        poison = await runner.enqueue(ctx, "renamed_away", {"n": 1})
        valid = await runner.enqueue(ctx, "double", {"n": 5})

        claimed = await runner.recover(ctx, max_concurrency=4)
        assert claimed == 2

        poisoned = await store.load(poison.run_id)
        assert poisoned is not None
        assert poisoned.status is DurableRunStatus.FAILED

        reloaded = await store.load(valid.run_id)
        assert reloaded is not None
        assert reloaded.status is DurableRunStatus.COMPLETED

    async def test_an_escaped_per_record_error_does_not_strand_the_batch(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # A failure that escapes the recorded-failure path itself (here: the terminal
        # write against the store errors) is swallowed per-record, so the co-claimed
        # runs still drain; the run stays leased RUNNING for a later sweep.
        ctx = context_from_modules(MockDepsModule())
        calls: list[int] = []
        registry = _double_registry(calls)

        async def boom(ctx: ExecutionContext, input_json: dict | None) -> dict:
            raise exc.internal("boom")

        registry.register("boom", boom)
        runner = DurableFunctionRunner(registry=registry)
        store = resolve_durable_run_store(ctx)

        poison = await runner.enqueue(ctx, "boom")
        valid = await runner.enqueue(ctx, "double", {"n": 2})

        original_fail = MockDurableRunStore.fail

        async def flaky_fail(
            self: MockDurableRunStore,
            run_id: str,
            *,
            error: str,
            fence: int | None = None,
        ) -> None:
            if run_id == poison.run_id:
                raise RuntimeError("store blip")

            await original_fail(self, run_id, error=error, fence=fence)

        monkeypatch.setattr(MockDurableRunStore, "fail", flaky_fail)

        claimed = await runner.recover(ctx)
        assert claimed == 2

        reloaded = await store.load(valid.run_id)
        assert reloaded is not None
        assert reloaded.status is DurableRunStatus.COMPLETED

        stranded = await store.load(poison.run_id)
        assert stranded is not None
        assert stranded.status is DurableRunStatus.RUNNING  # retried after lease expiry

    async def test_run_now_with_unregistered_name_fails_the_run_and_raises(
        self,
    ) -> None:
        ctx = context_from_modules(MockDepsModule())
        runner = DurableFunctionRunner(registry=DurableFunctionRegistry())
        store = resolve_durable_run_store(ctx)

        record = await runner.enqueue(ctx, "nope", idempotency_key="k1")

        with pytest.raises(CoreException, match="No durable function"):
            await runner.run_now(ctx, "nope", idempotency_key="k1")

        # The run is recorded FAILED — not left leased RUNNING forever.
        reloaded = await store.load(record.run_id)
        assert reloaded is not None
        assert reloaded.status is DurableRunStatus.FAILED
        assert "No durable function" in (reloaded.error or "")


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
