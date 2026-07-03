"""P6 multi-worker durability: fencing, delayed runs, and bounded-concurrency recovery.

# covers: DurableRunStorePort.complete
# covers: DurableRunStorePort.claim_abandoned
# covers: DurableFunctionRunner.recover
"""

from __future__ import annotations

import asyncio
from datetime import timedelta

from forze.application.contracts.durable.function import DurableRunStatus
from forze.application.execution import ExecutionContext
from forze.base.primitives import utcnow
from tests.support.execution_context import context_from_modules

from forze_kits.integrations.durable import (
    DurableFunctionRegistry,
    DurableFunctionRunner,
    resolve_durable_run_store,
)
from forze_mock import MockDepsModule, MockDurableRunStore, MockState

# ----------------------- #


class TestFencing:
    async def test_stale_worker_cannot_finish_after_reclaim(self) -> None:
        state = MockState()
        store = MockDurableRunStore(state=state)

        record = await store.enqueue("fn", input_json=None)
        worker_a = await store.begin(record.run_id, lease_for=timedelta(minutes=5))
        assert worker_a is not None and worker_a.attempts == 1

        # Worker A stalls; its lease expires and worker B reclaims the run (attempts -> 2).
        state.durable_runs[record.run_id]["leased_until"] = utcnow() - timedelta(hours=1)
        reclaimed = await store.claim_abandoned(limit=10, lease_for=timedelta(minutes=5))
        worker_b = next(r for r in reclaimed if r.run_id == record.run_id)
        assert worker_b.attempts == 2

        # Worker A wakes up and tries to finish with its stale fence — rejected (no-op).
        await store.complete(
            record.run_id, output_json={"by": "A"}, fence=worker_a.attempts
        )
        loaded = await store.load(record.run_id)
        assert loaded is not None
        assert loaded.status is DurableRunStatus.RUNNING
        assert loaded.output_json is None

        # Worker B, the current lease holder, completes it — wins.
        await store.complete(
            record.run_id, output_json={"by": "B"}, fence=worker_b.attempts
        )
        loaded = await store.load(record.run_id)
        assert loaded is not None
        assert loaded.status is DurableRunStatus.COMPLETED
        assert loaded.output_json == {"by": "B"}


class TestDelayedRuns:
    async def test_pending_run_not_claimed_until_due(self) -> None:
        state = MockState()
        store = MockDurableRunStore(state=state)

        future = await store.enqueue(
            "fn", input_json=None, available_at=utcnow() + timedelta(hours=1)
        )
        due = await store.enqueue(
            "fn", input_json=None, available_at=utcnow() - timedelta(minutes=1)
        )
        immediate = await store.enqueue("fn", input_json=None)  # available_at unset

        claimed = {
            r.run_id
            for r in await store.claim_abandoned(limit=10, lease_for=timedelta(minutes=5))
        }

        assert future.run_id not in claimed  # not yet due
        assert due.run_id in claimed
        assert immediate.run_id in claimed


class TestBoundedConcurrency:
    async def test_recover_bounds_concurrent_executions(self) -> None:
        state = MockState()
        ctx = context_from_modules(MockDepsModule(state=state))
        inflight = {"current": 0, "peak": 0}
        registry = DurableFunctionRegistry()

        async def handler(ctx: ExecutionContext, input_json: dict | None) -> dict:
            inflight["current"] += 1
            inflight["peak"] = max(inflight["peak"], inflight["current"])
            await asyncio.sleep(0.02)
            inflight["current"] -= 1
            return {"ok": True}

        registry.register("fn", handler)
        runner = DurableFunctionRunner(registry=registry)

        for _ in range(5):
            await runner.enqueue(ctx, "fn")

        recovered = await runner.recover(ctx, limit=10, max_concurrency=2)

        assert recovered == 5
        assert inflight["peak"] <= 2  # never more than the bound in flight at once

        store = resolve_durable_run_store(ctx)
        for data in state.durable_runs.values():
            reloaded = await store.load(data["run_id"])
            assert reloaded is not None
            assert reloaded.status is DurableRunStatus.COMPLETED
