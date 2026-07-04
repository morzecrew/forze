"""Lease heartbeat: a body that outlives its lease keeps the run leased and is not
reclaimed mid-flight (which would double-execute its side effects), and a body whose lease
*was* reclaimed is aborted rather than run to completion.

# covers: DurableRunStorePort.renew
# covers: DurableFunctionRunner.run_now
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


class TestStoreRenew:
    async def test_renew_extends_lease_and_blocks_reclaim_while_held(self) -> None:
        state = MockState()
        store = MockDurableRunStore(state=state)

        record = await store.enqueue("fn", input_json=None)
        claimed = await store.begin(
            record.run_id, lease_for=timedelta(milliseconds=10)
        )
        assert claimed is not None and claimed.attempts == 1

        # The short lease is already effectively expired; a renew by the current holder
        # pushes ``leased_until`` far into the future.
        held = await store.renew(
            record.run_id, lease_for=timedelta(minutes=5), fence=claimed.attempts
        )
        assert held is True

        # With the lease live again the recovery scanner finds nothing to reclaim, so the
        # run is not stolen out from under the still-executing worker.
        reclaimed = await store.claim_abandoned(
            limit=10, lease_for=timedelta(minutes=5)
        )
        assert reclaimed == []

        loaded = await store.load(record.run_id)
        assert loaded is not None
        assert loaded.status is DurableRunStatus.RUNNING
        assert loaded.attempts == 1  # never reclaimed

    async def test_renew_with_stale_fence_reports_lost_lease(self) -> None:
        state = MockState()
        store = MockDurableRunStore(state=state)

        record = await store.enqueue("fn", input_json=None)
        worker_a = await store.begin(record.run_id, lease_for=timedelta(minutes=5))
        assert worker_a is not None and worker_a.attempts == 1

        # Worker A stalls; its lease expires and worker B reclaims (attempts -> 2).
        state.durable_runs[record.run_id]["leased_until"] = utcnow() - timedelta(hours=1)
        reclaimed = await store.claim_abandoned(
            limit=10, lease_for=timedelta(minutes=5)
        )
        worker_b = next(r for r in reclaimed if r.run_id == record.run_id)
        assert worker_b.attempts == 2

        # Worker A's heartbeat can no longer renew: its fence is stale, so it learns it must
        # stop rather than extend a lease it no longer owns.
        held = await store.renew(
            record.run_id, lease_for=timedelta(minutes=5), fence=worker_a.attempts
        )
        assert held is False

        # Worker B, the current holder, still renews successfully.
        held_b = await store.renew(
            record.run_id, lease_for=timedelta(minutes=5), fence=worker_b.attempts
        )
        assert held_b is True


class TestRunnerHeartbeat:
    async def test_runner_stops_body_when_lease_reclaimed(self) -> None:
        state = MockState()
        ctx = context_from_modules(MockDepsModule(state=state))
        store = resolve_durable_run_store(ctx)

        side_effects = {"count": 0}
        other_worker = {"attempts": None}
        triggered = {"done": False}

        # Enqueue up front (converge via idempotency key) so the handler closure knows the
        # run id and can simulate a second worker reclaiming the lease mid-body.
        record = await store.enqueue("fn", input_json=None, idempotency_key="k")
        run_id = record.run_id

        registry = DurableFunctionRegistry()

        async def handler(ctx: ExecutionContext, input_json: dict | None) -> dict:
            if not triggered["done"]:
                triggered["done"] = True
                # A second worker reclaims this run: expire the lease and let
                # claim_abandoned advance ``attempts`` (fence 1 -> 2).
                state.durable_runs[run_id]["leased_until"] = utcnow() - timedelta(hours=1)
                reclaimed = await store.claim_abandoned(
                    limit=10, lease_for=timedelta(minutes=5)
                )
                other_worker["attempts"] = next(
                    r.attempts for r in reclaimed if r.run_id == run_id
                )

            # Long enough that the heartbeat fires, fails to renew (stale fence), and aborts
            # us before this side effect can run.
            await asyncio.sleep(1.0)
            side_effects["count"] += 1  # must NOT execute — the body is cancelled first
            return {"ok": True}

        registry.register("fn", handler)
        runner = DurableFunctionRunner(
            registry=registry,
            lease_for=timedelta(milliseconds=60),
            heartbeat_divisor=2,
        )

        result = await runner.run_now(ctx, "fn", idempotency_key="k")

        # The reclaimed body was aborted before its side effect: exactly-once side effects.
        assert side_effects["count"] == 0
        assert other_worker["attempts"] == 2

        # Our worker wrote no terminal state (its fence was stale); the run is left RUNNING
        # under the new owner's claim.
        assert result.status is DurableRunStatus.RUNNING
        assert result.attempts == 2

    async def test_renew_error_is_treated_as_lease_loss(self) -> None:
        # A renewal that ERRORS (DB/network blip) must not crash the run with the raw error or
        # override the body result; it is treated as lease loss — the body is cancelled before
        # it double-executes, and the run is left RUNNING for a later recovery.
        from unittest.mock import AsyncMock, patch

        state = MockState()
        ctx = context_from_modules(MockDepsModule(state=state))

        side_effects = {"count": 0}

        registry = DurableFunctionRegistry()

        async def handler(ctx: ExecutionContext, input_json: dict | None) -> dict:
            # Outlive the heartbeat interval so a renewal fires (and raises) before we finish.
            await asyncio.sleep(1.0)
            side_effects["count"] += 1  # must NOT run — the body is cancelled on lease loss
            return {"ok": True}

        registry.register("fn", handler)
        runner = DurableFunctionRunner(
            registry=registry,
            lease_for=timedelta(milliseconds=60),
            heartbeat_divisor=2,
        )

        with patch.object(
            MockDurableRunStore,
            "renew",
            AsyncMock(side_effect=RuntimeError("db down")),
        ):
            # No RuntimeError escapes: the renewal failure is absorbed as lease loss.
            result = await runner.run_now(ctx, "fn")

        assert side_effects["count"] == 0
        # Left RUNNING (no terminal write) for a later recovery, not FAILED with the DB error.
        assert result.status is DurableRunStatus.RUNNING

    async def test_side_effect_runs_once_across_would_be_reclaim_window(self) -> None:
        state = MockState()
        ctx = context_from_modules(MockDepsModule(state=state))

        side_effects = {"count": 0}
        started = asyncio.Event()

        registry = DurableFunctionRegistry()

        async def handler(ctx: ExecutionContext, input_json: dict | None) -> dict:
            started.set()
            # Far longer than one lease: without a heartbeat the recovery scanner would
            # reclaim this run and re-run the body, double-counting the side effect.
            await asyncio.sleep(0.6)
            side_effects["count"] += 1
            return {"ok": True}

        registry.register("fn", handler)
        runner = DurableFunctionRunner(
            registry=registry,
            lease_for=timedelta(milliseconds=100),
            heartbeat_divisor=2,
        )

        run_task = asyncio.ensure_future(runner.run_now(ctx, "fn"))
        await started.wait()

        # Past the original lease, a recovery scan tries to reclaim the still-running run;
        # the heartbeat kept the lease live, so there is nothing to reclaim.
        await asyncio.sleep(0.3)
        recovered = await runner.recover(ctx, limit=10)
        assert recovered == 0

        result = await run_task
        assert result.status is DurableRunStatus.COMPLETED
        assert side_effects["count"] == 1  # ran exactly once, never reclaimed
