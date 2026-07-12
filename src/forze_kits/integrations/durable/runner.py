"""Durable-function runner: enqueue, execute-in-process, and recover abandoned runs."""

from __future__ import annotations

import asyncio
from contextlib import nullcontext, suppress
from datetime import datetime, timedelta
from time import perf_counter
from typing import TYPE_CHECKING, final
from uuid import UUID

import attrs

from forze.application.contracts.durable.function import (
    DurableRunContext,
    DurableRunRecord,
    DurableRunStatus,
    DurableRunStorePort,
    bind_durable_run,
    reset_durable_run,
)
from forze.application.contracts.tenancy import TenantIdentity
from forze.base.exceptions import CoreException, exc

from .._logger import logger
from ._resolve import resolve_durable_run_store
from .registry import DurableFunctionRegistry
from .telemetry import DurableTelemetry

if TYPE_CHECKING:
    from opentelemetry.trace import Span

    from forze.application.execution.context import ExecutionContext
    from forze.base.primitives import JsonDict

    from .registry import DurableFunctionHandler

# ----------------------- #

_FORWARD_INCOMPLETE_CODE = "saga.forward_incomplete"
"""A saga that committed at its pivot but could not complete forward — a distinct terminal
state from an ordinary failure (no compensation happened; manual completion is required)."""


@final
class _LeaseLost(Exception):
    """Raised inside ``_execute_bound`` when a heartbeat renewal reports the lease was
    reclaimed (another worker advanced ``attempts``). It aborts the body so the run does not
    keep double-executing the new owner's work; the new owner records the terminal state."""


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

    heartbeat_divisor: int = 3
    """Renew the lease every ``lease_for / heartbeat_divisor`` while a body runs, so a body
    that legitimately outlives one lease is not reclaimed mid-flight. Must be ``>= 2`` so a
    renewal lands before the lease expires (a single missed heartbeat still leaves headroom)."""

    max_run_duration: timedelta | None = timedelta(hours=1)
    """Cap on how long a single body may execute before the runner stops treating it as
    live: the body task is cancelled, heartbeat renewal stops, and the run lands ``FAILED``
    with the deadline reason. Without a cap a body hung on a dead peer heartbeats its lease
    alive forever — never reclaimed, pinning a recovery slot on this replica. The body is
    cancelled while the lease is still held, so nothing double-executes; re-enqueue to retry.
    Must comfortably exceed the longest legitimate body; ``None`` removes the cap (and
    restores the hang hazard)."""

    telemetry: DurableTelemetry | None = None
    """Optional OpenTelemetry spans + metrics for run execution and recovery."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if (
            self.max_run_duration is not None
            and self.max_run_duration.total_seconds() <= 0
        ):
            raise exc.configuration(
                "Max run duration must be positive (None disables the cap)"
            )

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
                await self._recover_one(ctx, record)

            return len(claimed)

        semaphore = asyncio.Semaphore(max_concurrency)

        async def _bounded(record: DurableRunRecord) -> None:
            async with semaphore:
                await self._recover_one(ctx, record)

        await asyncio.gather(*(_bounded(record) for record in claimed))

        return len(claimed)

    # ....................... #

    async def _recover_one(
        self,
        ctx: ExecutionContext,
        record: DurableRunRecord,
    ) -> None:
        try:
            await self._execute(ctx, record, reraise=False)

        except Exception:  # noqa: BLE001 — one bad run must not strand the batch
            # ``_execute`` records body failures itself; anything reaching here escaped
            # that path (a terminal write against the store errored, tenant binding
            # failed, ...). Swallow it so the co-claimed runs still drain — the run
            # stays leased RUNNING and a later sweep re-claims it after lease expiry.
            logger.exception(
                "Durable run %s (%s) escaped its failure path during recovery; "
                "continuing with the rest of the claimed batch",
                record.run_id,
                record.name,
            )

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
                    # Resolved inside the failure-handled region: a run whose name is
                    # no longer registered (deploy skew, a renamed function, a stale
                    # schedule) must land in FAILED like any other failing run — the
                    # scanner claims oldest-first, so letting it escape would strand
                    # every run co-claimed with it as leased RUNNING, sweep after sweep.
                    handler = self.registry.get(record.name)
                    output = await self._run_body_with_heartbeat(
                        ctx, store, handler, record, fence
                    )

                except _LeaseLost:
                    # A heartbeat found the lease reclaimed mid-body: another worker owns the
                    # run now. Stop without a terminal write (a fenced write would be a no-op
                    # anyway) and let the new owner record the outcome — this is the whole
                    # point of the heartbeat: not double-executing the body to completion.
                    outcome = "reclaimed"

                    return

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

    async def _run_body_with_heartbeat(
        self,
        ctx: ExecutionContext,
        store: DurableRunStorePort,
        handler: DurableFunctionHandler,
        record: DurableRunRecord,
        fence: int,
    ) -> JsonDict | None:
        # Run the body as its own task and renew the lease alongside it, so a body that
        # legitimately outlives one lease keeps the run leased instead of being reclaimed
        # mid-flight (which would double-execute its side effects). If a renewal reports the
        # lease was reclaimed, the heartbeat cancels the body and we surface ``_LeaseLost``.
        # A deadline watchdog bounds the whole execution: a hung body must not heartbeat its
        # lease alive forever, pinning a recovery slot on this replica.
        body = asyncio.ensure_future(handler(ctx, record.input_json))
        reclaimed = asyncio.Event()
        expired = asyncio.Event()
        watchers = [
            asyncio.ensure_future(
                self._heartbeat(store, record.run_id, fence, body, reclaimed)
            )
        ]

        if self.max_run_duration is not None:
            watchers.append(
                asyncio.ensure_future(
                    self._expire_body_after(self.max_run_duration, body, expired)
                )
            )

        try:
            return await body

        except asyncio.CancelledError:
            # A cancel raised because the heartbeat reclaimed the run is turned into
            # ``_LeaseLost``; a deadline-watchdog cancel becomes a recorded failure; an
            # external cancel (neither flag set) propagates untouched.
            if reclaimed.is_set():
                raise _LeaseLost from None

            if expired.is_set():
                # The body was cancelled while the lease was still held (no lapse, so no
                # double-execution) and the run lands FAILED — there is no retry machinery
                # here; an operator re-enqueues.
                raise exc.timeout(
                    f"Durable run {record.run_id} ({record.name}) exceeded "
                    f"max_run_duration ({self.max_run_duration}); the body was cancelled "
                    "before its lease could lapse — re-enqueue to retry",
                ) from None

            raise

        finally:
            for watcher in watchers:
                watcher.cancel()

            for watcher in watchers:
                with suppress(asyncio.CancelledError):
                    await watcher

            if not body.done():
                # Only reachable when the awaiter itself was cancelled (a shutdown drain,
                # an enclosing timeout): tear the body down with it and wait for its
                # unwind, or it keeps executing — and heartbeating — detached.
                body.cancel()

                with suppress(asyncio.CancelledError):
                    await body

    # ....................... #

    async def _heartbeat(
        self,
        store: DurableRunStorePort,
        run_id: str,
        fence: int,
        body: "asyncio.Future[JsonDict | None]",
        reclaimed: asyncio.Event,
    ) -> None:
        interval = self.lease_for / max(self.heartbeat_divisor, 2)
        seconds = interval.total_seconds()

        while True:
            await asyncio.sleep(seconds)

            try:
                held = await store.renew(
                    run_id, lease_for=self.lease_for, fence=fence
                )

            except Exception:
                # A renewal that errors (DB/network blip) means we can no longer prove we hold
                # the lease; another worker may reclaim it. Treat it as lease loss — stop the
                # body before it double-executes and surface the lease-loss path — rather than
                # letting the raw error escape the heartbeat task and override the body result.
                # (``Exception`` leaves a genuine task cancellation — ``CancelledError`` — to
                # propagate so the ``finally`` cancel path still works.)
                logger.warning(
                    "Durable run %s heartbeat renewal errored; treating as lease loss",
                    run_id,
                    exc_info=True,
                )
                reclaimed.set()
                body.cancel()

                return

            if not held:
                # Another worker reclaimed the run; stop the body before it double-executes.
                reclaimed.set()
                body.cancel()

                return

    # ....................... #

    async def _expire_body_after(
        self,
        cap: timedelta,
        body: "asyncio.Future[JsonDict | None]",
        expired: asyncio.Event,
    ) -> None:
        await asyncio.sleep(cap.total_seconds())

        # Past the cap the body is no longer treated as live: cancel it (the same teardown
        # the heartbeat uses on lease loss) so the run frees its recovery slot instead of
        # renewing its lease forever.
        expired.set()
        body.cancel()

    # ....................... #

    def _mark_span_error(self, span: "Span | None", error: BaseException) -> None:
        if self.telemetry is not None and span is not None:
            self.telemetry.mark_error(span, error)
