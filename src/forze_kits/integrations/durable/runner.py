"""Durable-function runner: enqueue, execute-in-process, and recover abandoned runs."""

from __future__ import annotations

import asyncio
from contextlib import nullcontext
from datetime import datetime, timedelta
from time import perf_counter
from typing import TYPE_CHECKING, final
from uuid import UUID

import attrs

from forze.application.contracts.durable.function import (
    DurableRunContext,
    DurableRunRecord,
    DurableRunStatus,
    bind_durable_run,
    reset_durable_run,
)
from forze.application.contracts.tenancy import TenantIdentity
from forze.base.exceptions import CoreException

from ._resolve import resolve_durable_run_store
from .registry import DurableFunctionRegistry
from .telemetry import DurableTelemetry

if TYPE_CHECKING:
    from opentelemetry.trace import Span

    from forze.application.execution.context import ExecutionContext
    from forze.base.primitives import JsonDict

# ----------------------- #

_FORWARD_INCOMPLETE_CODE = "saga.forward_incomplete"
"""A saga that committed at its pivot but could not complete forward — a distinct terminal
state from an ordinary failure (no compensation happened; manual completion is required)."""


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class DurableFunctionRunner:
    """Drive durable functions over a :class:`DurableRunStorePort` and the step journal.

    ``enqueue`` records a run; ``run_now`` records and executes it in-process; ``recover``
    re-claims abandoned runs (crashed mid-flight) and re-invokes them — completed steps
    replay from the journal rather than re-running (exactly-once for the recorded result; a
    body may still re-run if a worker is reclaimed / crashes before it journals, so keep step
    bodies idempotent).
    """

    registry: DurableFunctionRegistry
    """Name → durable-function body (must contain a run's ``name`` to execute/recover it)."""

    lease_for: timedelta = timedelta(minutes=5)
    """How long a claim leases a run before the recovery scanner may reclaim it."""

    telemetry: DurableTelemetry | None = None
    """Optional OpenTelemetry spans + metrics for run execution and recovery."""

    # ....................... #

    async def enqueue(
        self,
        ctx: ExecutionContext,
        name: str,
        input_json: JsonDict | None = None,
        *,
        idempotency_key: str | None = None,
        tenant_id: UUID | None = None,
        run_at: datetime | None = None,
    ) -> DurableRunRecord:
        """Record a new ``PENDING`` run (idempotency-key re-submits converge on one run).

        *run_at* delays when the recovery scan may claim it (a scheduled/delayed run).
        """

        store = resolve_durable_run_store(ctx)

        return await store.enqueue(
            name,
            input_json=input_json,
            idempotency_key=idempotency_key,
            tenant_id=tenant_id,
            available_at=run_at,
        )

    # ....................... #

    async def run_now(
        self,
        ctx: ExecutionContext,
        name: str,
        input_json: JsonDict | None = None,
        *,
        idempotency_key: str | None = None,
        tenant_id: UUID | None = None,
    ) -> DurableRunRecord:
        """Enqueue and execute a run in-process; return its final record.

        An idempotent re-submit that already completed returns immediately; a run already
        claimed elsewhere is returned as-is (not double-executed).
        """

        store = resolve_durable_run_store(ctx)
        record = await store.enqueue(
            name,
            input_json=input_json,
            idempotency_key=idempotency_key,
            tenant_id=tenant_id,
        )

        if record.status is DurableRunStatus.COMPLETED:
            return record

        claimed = await store.begin(record.run_id, lease_for=self.lease_for)

        if claimed is None:
            return await store.load(record.run_id) or record

        await self._execute(ctx, claimed, reraise=True)

        return await store.load(claimed.run_id) or claimed

    # ....................... #

    async def recover(
        self,
        ctx: ExecutionContext,
        *,
        limit: int = 10,
        max_concurrency: int | None = None,
    ) -> int:
        """Claim up to *limit* abandoned runs and re-invoke them; return the count claimed.

        A body failure during recovery is recorded on the run and swallowed (the scanner
        keeps draining), never propagated. With *max_concurrency* set the claimed runs are
        recovered concurrently up to that bound (each run executes in its own task, so its
        ambient run binding stays isolated); ``None`` recovers them sequentially.
        """

        store = resolve_durable_run_store(ctx)
        claimed = await store.claim_abandoned(limit=limit, lease_for=self.lease_for)

        if not claimed:
            return 0

        if self.telemetry is not None:
            self.telemetry.record_recovered(len(claimed))

        if max_concurrency is None or max_concurrency <= 1:
            for record in claimed:
                await self._execute(ctx, record, reraise=False)

            return len(claimed)

        semaphore = asyncio.Semaphore(max_concurrency)

        async def _bounded(record: DurableRunRecord) -> None:
            async with semaphore:
                await self._execute(ctx, record, reraise=False)

        await asyncio.gather(*(_bounded(record) for record in claimed))

        return len(claimed)

    # ....................... #

    async def _execute(
        self,
        ctx: ExecutionContext,
        record: DurableRunRecord,
        *,
        reraise: bool,
    ) -> None:
        # Execute under the run's tenant so the step journal + terminal writes resolve the
        # right tenant (essential when recovery ran unbound over a tagged table; a no-op
        # under a namespace shard already bound to this tenant).
        binding = (
            ctx.inv_ctx.bind_identity(
                tenant=TenantIdentity(tenant_id=record.tenant_id)
            )
            if record.tenant_id is not None
            else nullcontext()
        )

        with binding:
            await self._execute_bound(ctx, record, reraise=reraise)

    # ....................... #

    async def _execute_bound(
        self,
        ctx: ExecutionContext,
        record: DurableRunRecord,
        *,
        reraise: bool,
    ) -> None:
        store = resolve_durable_run_store(ctx)
        handler = self.registry.get(record.name)

        # The claim's attempt count is this execution's fence token: a stale worker whose
        # lease was reclaimed (attempts advanced) cannot finish the run out from under the
        # new owner.
        fence = record.attempts

        token = bind_durable_run(
            DurableRunContext(
                run_id=record.run_id,
                name=record.name,
                attempt=record.attempts,
            )
        )

        started = perf_counter()
        outcome = "completed"
        span_cm = (
            self.telemetry.run_span(record)
            if self.telemetry is not None
            else nullcontext()
        )

        try:
            with span_cm as span:
                try:
                    output = await handler(ctx, record.input_json)

                except CoreException as error:
                    # A pivot-committed saga that could not complete forward is a distinct
                    # terminal state (never compensated, finished by hand) — not a failure.
                    outcome = (
                        "forward_incomplete"
                        if (error.code or "") == _FORWARD_INCOMPLETE_CODE
                        else "failed"
                    )
                    self._mark_span_error(span, error)

                    if outcome == "forward_incomplete":
                        await store.mark_forward_incomplete(
                            record.run_id, error=str(error), fence=fence
                        )
                    else:
                        await store.fail(record.run_id, error=str(error), fence=fence)

                    if reraise:
                        raise

                    return

                except Exception as error:  # noqa: BLE001 — record then optionally re-raise
                    outcome = "failed"
                    self._mark_span_error(span, error)
                    await store.fail(record.run_id, error=str(error), fence=fence)

                    if reraise:
                        raise

                    return

                await store.complete(record.run_id, output_json=output, fence=fence)

        finally:
            reset_durable_run(token)

            if self.telemetry is not None:
                self.telemetry.record_run(
                    record.name, outcome, (perf_counter() - started) * 1000.0
                )

    # ....................... #

    def _mark_span_error(self, span: "Span | None", error: BaseException) -> None:
        if self.telemetry is not None and span is not None:
            self.telemetry.mark_error(span, error)
