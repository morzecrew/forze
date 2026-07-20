"""Execution deadline: a hung body (stuck awaiting a dead peer, no deadline of its own) is
cancelled at ``max_run_duration`` instead of heartbeating its lease alive forever — the run
lands FAILED with the deadline reason, its recovery slot frees so the scanner keeps draining,
and nothing double-executes (the body was cancelled before any lease lapse).

# covers: DurableFunctionRunner.run_now
# covers: DurableFunctionRunner.recover
"""

from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest

from forze.application.contracts.durable.function import DurableRunStatus
from forze.application.execution import ExecutionContext
from forze.base.exceptions import CoreException, ExceptionKind
from forze_kits.integrations.durable import (
    DurableFunctionRegistry,
    DurableFunctionRunner,
    resolve_durable_run_store,
)
from forze_mock import MockDepsModule
from tests.support.execution_context import context_from_modules

# ----------------------- #


def _register_hung(
    registry: DurableFunctionRegistry, unwound: dict[str, bool]
) -> None:
    async def hung(ctx: ExecutionContext, input_json: dict | None) -> dict:
        try:
            # A dead peer: never set, and the body carries no deadline of its own.
            await asyncio.Event().wait()
        finally:
            unwound["body"] = True

        return {"ok": True}

    registry.register("hung", hung)


# ....................... #


class TestRunDeadline:
    async def test_hung_body_lands_failed_with_deadline_reason(self) -> None:
        ctx = context_from_modules(MockDepsModule())
        store = resolve_durable_run_store(ctx)

        unwound = {"body": False}
        registry = DurableFunctionRegistry()
        _register_hung(registry, unwound)

        runner = DurableFunctionRunner(
            registry=registry,
            max_run_duration=timedelta(milliseconds=50),
        )

        # Enqueue up front (converge via idempotency key) so the record is loadable after
        # ``run_now`` raises.
        record = await store.enqueue("hung", input_json=None, idempotency_key="k")

        before = asyncio.all_tasks()

        with pytest.raises(CoreException, match="max_run_duration") as caught:
            await runner.run_now(ctx, "hung", idempotency_key="k")

        assert caught.value.kind is ExceptionKind.TIMEOUT

        # The body was cancelled (its ``finally`` ran) — not left hanging detached.
        assert unwound["body"] is True

        # FAILED with the deadline reason: the body was cancelled before any lease lapse,
        # so no double-execution; there is no retry machinery — an operator re-enqueues.
        reloaded = await store.load(record.run_id)
        assert reloaded is not None
        assert reloaded.status is DurableRunStatus.FAILED
        assert "max_run_duration" in (reloaded.error or "")

        # Heartbeat, watchdog, and body are all torn down — no lingering task keeps
        # renewing the lease.
        leaked = {task for task in asyncio.all_tasks() - before if not task.done()}
        assert leaked == set()

    async def test_deadline_frees_the_slot_so_the_sweep_keeps_draining(self) -> None:
        # ``recover`` awaits each claimed body inline (and, bounded, holds a semaphore
        # slot per body): without a deadline one hung body stalls the whole sweep. With
        # the cap the hung run fails in place and the co-claimed run still completes.
        ctx = context_from_modules(MockDepsModule())
        store = resolve_durable_run_store(ctx)

        unwound = {"body": False}
        registry = DurableFunctionRegistry()
        _register_hung(registry, unwound)

        async def double(ctx: ExecutionContext, input_json: dict | None) -> dict:
            return {"doubled": (input_json or {}).get("n", 0) * 2}

        registry.register("double", double)

        runner = DurableFunctionRunner(
            registry=registry,
            max_run_duration=timedelta(milliseconds=50),
        )

        # The hung run is enqueued FIRST, so the sequential sweep executes it ahead of
        # the valid run.
        hung = await runner.enqueue(ctx, "hung")
        valid = await runner.enqueue(ctx, "double", {"n": 3})

        claimed = await runner.recover(ctx)
        assert claimed == 2

        failed = await store.load(hung.run_id)
        assert failed is not None
        assert failed.status is DurableRunStatus.FAILED
        assert "max_run_duration" in (failed.error or "")

        completed = await store.load(valid.run_id)
        assert completed is not None
        assert completed.status is DurableRunStatus.COMPLETED
        assert completed.output_json == {"doubled": 6}

        # The failed run is terminal: a second sweep has nothing to re-claim, so the
        # hung body cannot pin a slot sweep after sweep.
        assert await runner.recover(ctx) == 0

    async def test_deadline_frees_a_bounded_concurrency_slot(self) -> None:
        # With ``max_concurrency=1`` the hung body holds the only semaphore slot; the
        # deadline must release it so the co-claimed run still recovers.
        ctx = context_from_modules(MockDepsModule())
        store = resolve_durable_run_store(ctx)

        unwound = {"body": False}
        registry = DurableFunctionRegistry()
        _register_hung(registry, unwound)

        async def noop(ctx: ExecutionContext, input_json: dict | None) -> dict:
            return {"ok": True}

        registry.register("noop", noop)

        runner = DurableFunctionRunner(
            registry=registry,
            max_run_duration=timedelta(milliseconds=50),
        )

        await runner.enqueue(ctx, "hung")
        valid = await runner.enqueue(ctx, "noop")

        claimed = await runner.recover(ctx, max_concurrency=1)
        assert claimed == 2

        completed = await store.load(valid.run_id)
        assert completed is not None
        assert completed.status is DurableRunStatus.COMPLETED

    async def test_body_finishing_within_the_deadline_is_untouched(self) -> None:
        ctx = context_from_modules(MockDepsModule())
        registry = DurableFunctionRegistry()

        async def quick(ctx: ExecutionContext, input_json: dict | None) -> dict:
            return {"ok": True}

        registry.register("quick", quick)
        runner = DurableFunctionRunner(
            registry=registry,
            max_run_duration=timedelta(seconds=30),
        )

        record = await runner.run_now(ctx, "quick")
        assert record.status is DurableRunStatus.COMPLETED

    async def test_non_positive_max_run_duration_rejected(self) -> None:
        with pytest.raises(CoreException, match="positive"):
            DurableFunctionRunner(
                registry=DurableFunctionRegistry(),
                max_run_duration=timedelta(0),
            )


class TestDeadlineEdges:
    async def test_no_cap_when_max_run_duration_is_none(self) -> None:
        # ``None`` removes the cap: no watchdog is armed and a legitimate body
        # completes untouched.
        ctx = context_from_modules(MockDepsModule())

        registry = DurableFunctionRegistry()

        async def quick(ctx: ExecutionContext, input_json: dict | None) -> dict:
            return {"ok": True}

        registry.register("quick", quick)

        runner = DurableFunctionRunner(registry=registry, max_run_duration=None)

        record = await runner.run_now(ctx, "quick")
        assert record.status is DurableRunStatus.COMPLETED

    async def test_run_now_lost_claim_returns_the_other_workers_record(self) -> None:
        # A concurrent worker won the claim: ``run_now`` does not execute the body
        # twice — it returns the freshest loadable record.
        ctx = context_from_modules(MockDepsModule())
        store = resolve_durable_run_store(ctx)

        registry = DurableFunctionRegistry()
        calls = {"n": 0}

        async def once(ctx: ExecutionContext, input_json: dict | None) -> dict:
            calls["n"] += 1
            return {"ok": True}

        registry.register("once", once)
        runner = DurableFunctionRunner(registry=registry)

        record = await store.enqueue("once", input_json=None, idempotency_key="k")
        # Another worker holds the lease already.
        claimed = await store.begin(record.run_id, lease_for=timedelta(minutes=5))
        assert claimed is not None

        result = await runner.run_now(ctx, "once", idempotency_key="k")

        assert calls["n"] == 0  # the body never ran here
        assert result.run_id == record.run_id

    async def test_recovery_records_a_plain_exception_and_continues(self) -> None:
        # A non-CoreException body failure under recovery (reraise=False) lands
        # FAILED with the error recorded and does not abort the sweep.
        ctx = context_from_modules(MockDepsModule())
        store = resolve_durable_run_store(ctx)

        registry = DurableFunctionRegistry()

        async def broken(ctx: ExecutionContext, input_json: dict | None) -> dict:
            raise ValueError("plain failure")

        async def fine(ctx: ExecutionContext, input_json: dict | None) -> dict:
            return {"ok": True}

        registry.register("broken", broken)
        registry.register("fine", fine)
        runner = DurableFunctionRunner(registry=registry)

        bad = await store.enqueue("broken", input_json=None)
        good = await store.enqueue("fine", input_json=None)

        await runner.recover(ctx)

        reloaded_bad = await store.load(bad.run_id)
        reloaded_good = await store.load(good.run_id)
        assert reloaded_bad is not None and reloaded_good is not None
        assert reloaded_bad.status is DurableRunStatus.FAILED
        assert "plain failure" in (reloaded_bad.error or "")
        assert reloaded_good.status is DurableRunStatus.COMPLETED
