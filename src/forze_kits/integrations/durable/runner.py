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
    DurableCancelSignal,
    DurableRunContext,
    DurableRunRecord,
    DurableRunStatus,
    DurableRunStorePort,
    bind_durable_cancel_signal,
    bind_durable_run,
    durable_run_control_capabilities,
    reset_durable_cancel_signal,
    reset_durable_run,
)
from forze.application.contracts.saga import SAGA_CANCELLED_CODE
from forze.application.contracts.tenancy import TenantIdentity
from forze.base.exceptions import CoreException, exc

from .._logger import logger
from ._resolve import resolve_durable_run_admin, resolve_durable_run_store
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

_RUN_TIMED_OUT_CODE = "durable.run_timed_out"
"""A run cancelled by the deadline watchdog. Distinct from an ordinary failure: it sends an
operator to the cap or the workload's size, not to the body's code."""

_UNRECORDED_OUTCOME = "unrecorded"
"""Telemetry outcome for an execution whose terminal write never landed.

Not a state a *run* can be in. It marks the **attempt**: the body finished, and the store
could not be told, so the row is normally still ``RUNNING`` and recovery will re-claim it.
"Normally", because a write that committed and then lost its acknowledgement looks identical
from here — which is the point. The label reports what this worker *knows was recorded*, not
what it hopes happened, and its whole purpose is to stop a store outage from being drawn as
a wave of completions on the one dashboard someone is watching during the outage."""


@final
class _LeaseLost(Exception):
    """Raised inside ``_execute_bound`` when a heartbeat renewal reports the lease was
    reclaimed (another worker advanced ``attempts``). It aborts the body so the run does not
    keep double-executing the new owner's work; the new owner records the terminal state."""


@final
class _CancelRequested(Exception):
    """Raised inside ``_execute_bound`` when the body stopped because an operator asked it to.

    Cancellation is delivered to the body as an ordinary ``CancelledError``; this is the
    runner's translation of "that cancel was the one *we* caused" into the terminal write.
    Not an error — no ``reraise`` and no span error mark — so ``run_now`` returns the
    ``CANCELLED`` record rather than raising at its caller."""


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
    live: the body task is cancelled, heartbeat renewal stops, and the run lands
    ``TIMED_OUT`` with the deadline reason. Without a cap a body hung on a dead peer
    heartbeats its lease alive forever — never reclaimed, pinning a recovery slot on this
    replica. The body is cancelled while the lease is still held, so nothing
    double-executes; re-enqueue to retry. Must comfortably exceed the longest legitimate
    body; ``None`` removes the cap (and restores the hang hazard).

    The cap composes with cancellation rather than competing with it: a body that ignores an
    operator's stop is still bounded here, and because the *ask* was recorded first such a
    run lands ``CANCELLED`` — the operator's reason outranks the watchdog's."""

    telemetry: DurableTelemetry | None = None
    """Optional OpenTelemetry spans + metrics for run execution and recovery."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.max_run_duration is not None and self.max_run_duration.total_seconds() <= 0:
            raise exc.configuration("Max run duration must be positive (None disables the cap)")

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

    async def request_cancel(self, ctx: ExecutionContext, run_id: str) -> bool:
        """Ask a run to stop; return whether the ask was recorded.

        The operator entry point for the "Stop" button, over the control-plane admin port.
        **Cooperative**: a ``PENDING`` run stops at once (nothing is executing), while a
        ``RUNNING`` one keeps going until its holder observes the stamp on the next lease
        heartbeat — so observation latency is ``lease_for / heartbeat_divisor`` (100 s at
        stock settings), and a body that never awaits is bounded only by
        :attr:`max_run_duration`. A terminal run returns ``False``.

        Fails **closed** rather than silently: a backend that does not advertise
        ``supports_cancel`` cannot deliver the request to a running body, and accepting it
        anyway would make the button a lie.

        :raises CoreException: ``configuration`` when the wired backend cannot cancel.
        """

        admin = resolve_durable_run_admin(ctx)

        if not durable_run_control_capabilities(admin).supports_cancel:
            raise exc.configuration(
                f"The wired durable-run admin port ({type(admin).__name__}) does not "
                "support cancellation, so a cancel request would be accepted and dropped. "
                "Use a tier that reports supports_cancel (the self-hosted run store does), "
                "or stop the run through the engine's own controls.",
            )

        return await admin.request_cancel(run_id)

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

        except Exception:
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
            ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=record.tenant_id))
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

        # One signal per execution, shared by reference with the body's task: the heartbeat
        # writes to it, the saga executor reads it to tell an operator's cancel apart from a
        # drain, and this method reads back whatever the run decided.
        #
        # A record that already carries a refusal starts the signal **spent**: the ask is
        # still on the row, so the heartbeat would otherwise read it back and cancel the
        # body all over again on every recovery attempt.
        cancel = (
            DurableCancelSignal.already_refused()
            if record.cancel_refused_at is not None
            else DurableCancelSignal()
        )
        cancel_token = bind_durable_cancel_signal(cancel)

        started = perf_counter()

        # Assigned only once a terminal write has *landed*. A store outage is exactly when
        # the dashboard is being read, so an outcome recorded ahead of its write would count
        # a completion that never happened during the incident that stopped it — the run is
        # in fact still RUNNING and will be reclaimed. ``reclaimed`` is the one outcome set
        # without a write, because deciding not to write is what it means.
        outcome = _UNRECORDED_OUTCOME
        span_cm = self.telemetry.run_span(record) if self.telemetry is not None else nullcontext()

        try:
            with span_cm as span:
                if record.cancel_requested_at is not None and record.cancel_refused_at is None:
                    # The ask was already on the record when this claim took it: either the
                    # previous holder died with the stamp down, or the run was cancelled
                    # between enqueue and pickup. Land it without invoking the body —
                    # re-running work an operator already stopped is the one thing recovery
                    # must not do.
                    #
                    # A **refused** ask is excluded, and the exclusion is load-bearing: only a
                    # saga past its pivot refuses, so short-circuiting one would mark a run
                    # that committed at its point of no return as "cancelled, nothing wrong"
                    # and abandon it there. Past the pivot the run must be re-invoked and
                    # completed forward — replaying its journal — or land
                    # ``FORWARD_INCOMPLETE`` on its own merits.
                    await store.mark_cancelled(record.run_id, fence=fence)
                    outcome = "cancelled"

                    return

                try:
                    # Resolved inside the failure-handled region: a run whose name is
                    # no longer registered (deploy skew, a renamed function, a stale
                    # schedule) must land in FAILED like any other failing run — the
                    # scanner claims oldest-first, so letting it escape would strand
                    # every run co-claimed with it as leased RUNNING, sweep after sweep.
                    handler = self.registry.get(record.name)
                    output = await self._run_body_with_heartbeat(
                        ctx, store, handler, record, fence, cancel
                    )

                except _LeaseLost:
                    # A heartbeat found the lease reclaimed mid-body: another worker owns the
                    # run now. Stop without a terminal write (a fenced write would be a no-op
                    # anyway) and let the new owner record the outcome — this is the whole
                    # point of the heartbeat: not double-executing the body to completion.
                    outcome = "reclaimed"

                    return

                except _CancelRequested:
                    # The body stopped because an operator asked. Not an error: no span
                    # error mark, and never re-raised, so ``run_now`` hands its caller the
                    # CANCELLED record instead of an exception it did not cause.
                    await store.mark_cancelled(record.run_id, fence=fence)
                    outcome = "cancelled"

                    return

                except CoreException as error:
                    landed = self._core_outcome(error)

                    # A cancelled saga rides in on an exception because that is how the
                    # executor unwinds after compensating — but it is still an operator's
                    # ask, so it is neither marked on the span nor re-raised at the caller.
                    if landed != "cancelled":
                        self._mark_span_error(span, error)

                    await self._record_terminal(store, record.run_id, fence, landed, error)
                    outcome = landed

                    if reraise and landed != "cancelled":
                        raise

                    return

                except Exception as error:
                    self._mark_span_error(span, error)
                    await store.fail(record.run_id, error=str(error), fence=fence)
                    outcome = "failed"

                    if reraise:
                        raise

                    return

                await store.complete(record.run_id, output_json=output, fence=fence)
                outcome = "completed"

        finally:
            reset_durable_cancel_signal(cancel_token)
            reset_durable_run(token)

            if cancel.refused:
                # A saga past its pivot observed the ask and declined it. Recorded at the end
                # rather than at observation time: the refusing code is the saga executor,
                # which holds no run store and no fence. Until this lands, the run reads as
                # "asked, still running" — which is the truth in the interval.
                #
                # Swallowed on failure, like the heartbeat's renewal and the recovery
                # scanner's escape hatch: this runs in a ``finally``, so an error here would
                # replace whatever outcome is propagating (a body's real exception, for a
                # ``reraise`` caller) and skip the telemetry below — destroying the run's
                # actual result to report a stamp that is only advisory.
                try:
                    await store.refuse_cancel(record.run_id, fence=fence)

                except Exception:
                    logger.warning(
                        "Durable run %s refused a cancel request but the refusal could not "
                        "be recorded; the run's outcome stands and is reported normally",
                        record.run_id,
                        exc_info=True,
                    )

            if self.telemetry is not None:
                self.telemetry.record_run(record.name, outcome, (perf_counter() - started) * 1000.0)

    # ....................... #

    def _core_outcome(self, error: CoreException) -> str:
        """Classify a ``CoreException`` escaping a body into its terminal outcome.

        Three codes buy their own terminal state, because each sends an operator somewhere
        different: to the saga's committed pivot, to the deadline cap, or to nobody at all
        (they asked for it). Everything else is a failure.
        """

        return {
            _FORWARD_INCOMPLETE_CODE: "forward_incomplete",
            _RUN_TIMED_OUT_CODE: "timed_out",
            SAGA_CANCELLED_CODE: "cancelled",
        }.get(error.code or "", "failed")

    # ....................... #

    async def _record_terminal(
        self,
        store: DurableRunStorePort,
        run_id: str,
        fence: int,
        outcome: str,
        error: CoreException,
    ) -> None:
        if outcome == "forward_incomplete":
            await store.mark_forward_incomplete(run_id, error=str(error), fence=fence)

        elif outcome == "timed_out":
            await store.mark_timed_out(run_id, error=str(error), fence=fence)

        elif outcome == "cancelled":
            await store.mark_cancelled(run_id, error=str(error), fence=fence)

        else:
            await store.fail(run_id, error=str(error), fence=fence)

    # ....................... #

    async def _run_body_with_heartbeat(
        self,
        ctx: ExecutionContext,
        store: DurableRunStorePort,
        handler: DurableFunctionHandler,
        record: DurableRunRecord,
        fence: int,
        cancel: DurableCancelSignal,
    ) -> JsonDict | None:
        # Run the body as its own task and renew the lease alongside it, so a body that
        # legitimately outlives one lease keeps the run leased instead of being reclaimed
        # mid-flight (which would double-execute its side effects). If a renewal reports the
        # lease was reclaimed, the heartbeat cancels the body and we surface ``_LeaseLost``.
        # The same renewal carries an operator's cancel request, which tears the body down
        # the same way. A deadline watchdog bounds the whole execution: a hung body must not
        # heartbeat its lease alive forever, pinning a recovery slot on this replica.
        body = asyncio.ensure_future(handler(ctx, record.input_json))
        reclaimed = asyncio.Event()
        expired = asyncio.Event()
        watchers = [
            asyncio.ensure_future(
                self._heartbeat(store, record.run_id, fence, body, reclaimed, cancel)
            )
        ]

        if self.max_run_duration is not None:
            watchers.append(
                asyncio.ensure_future(self._expire_body_after(self.max_run_duration, body, expired))
            )

        try:
            return await body

        except Exception:
            # A body may convert the cancellation we delivered into an exception of its own —
            # the saga executor turns an interrupted rollback into ``saga.compensation_failed``
            # so a partial rollback is not reported as a clean stop. That conversion must not
            # outrank a lease loss: if the heartbeat says we no longer hold the run, the
            # outcome is not ours to record. Writing it anyway is a no-op when another worker
            # already claimed the run, but a renewal *error* leaves the fence valid — and then
            # a transient blip during a rollback would terminally FAIL a run that recovery
            # should have replayed.
            if reclaimed.is_set():
                raise _LeaseLost from None

            raise

        except asyncio.CancelledError:
            # Four causes arrive as one exception, so they are told apart by which watcher
            # fired. A reclaim wins outright: without the lease nothing we write lands. An
            # operator's ask outranks the deadline — the two watchdogs compose, and a body
            # that ignored the ask until the cap ran out was still stopped because somebody
            # asked. An external cancel (no flag set) propagates untouched.
            if reclaimed.is_set():
                raise _LeaseLost from None

            if cancel.requested and not cancel.refused:
                raise _CancelRequested from None

            if expired.is_set():
                # ``cancel`` only learns about an ask on a heartbeat *tick*, so an ask
                # recorded since the last one is invisible here — a window of up to one
                # interval (100 s at stock settings). The contract orders these two causes by
                # when the ask was **recorded**, not by when this process noticed, so confirm
                # against the row before calling an earlier ask a timeout.
                #
                # Skipped once this run has already refused: that decision lives only in the
                # in-memory signal until the ``finally`` persists it, so the row would still
                # read as an unanswered ask and turn a genuine deadline into a cancellation.
                if not cancel.refused and await self._cancel_pending_on_record(
                    store, record.run_id
                ):
                    raise _CancelRequested from None

                # The body was cancelled while the lease was still held (no lapse, so no
                # double-execution) and the run lands TIMED_OUT — distinct from FAILED
                # because it sends you to the cap, not to the body's code. There is no
                # retry machinery here; an operator re-enqueues.
                raise exc.timeout(
                    f"Durable run {record.run_id} ({record.name}) exceeded "
                    f"max_run_duration ({self.max_run_duration}); the body was cancelled "
                    "before its lease could lapse — re-enqueue to retry",
                    code=_RUN_TIMED_OUT_CODE,
                ) from None

            raise

        finally:
            for watcher in watchers:
                watcher.cancel()

            for watcher in watchers:
                with suppress(asyncio.CancelledError):
                    await watcher

            if not body.done():  # pragma: no cover — _must_cancel timing edge
                # Only reachable when the awaiter's cancellation was delivered at the
                # exact await boundary without cancelling the body first (an asyncio
                # ``_must_cancel`` edge; the normal path cancels the body through the
                # awaiter's own cancellation): tear the body down and wait for its
                # unwind, or it keeps executing — and heartbeating — detached.
                body.cancel()

                with suppress(asyncio.CancelledError):
                    await body

    # ....................... #

    def _heartbeat_seconds(self) -> float:
        """The lease-renewal cadence, and the bound on any store call made alongside a body.

        ``heartbeat_divisor`` is floored at 2 so a renewal always lands before the lease
        expires; a single missed beat still leaves headroom.
        """

        return (self.lease_for / max(self.heartbeat_divisor, 2)).total_seconds()

    # ....................... #

    async def _cancel_pending_on_record(
        self,
        store: DurableRunStorePort,
        run_id: str,
    ) -> bool:
        """Whether the row carries an ask this execution has not already settled.

        One read, only on the deadline path, to close the gap between an ask being recorded
        and the next heartbeat reading it back. A refused ask does not count: the run decided
        to keep going, so a later deadline is the deadline's own.
        """

        try:
            # Bounded like the heartbeat's own renewal, and for the same reason turned
            # inside out: this read runs *after* the deadline fired, on the path whose entire
            # job is to free the recovery slot. Left unbounded, a hung connection would hold
            # the run ``RUNNING`` indefinitely — the deadline's guarantee defeated by the
            # check that decides what to call the deadline.
            async with asyncio.timeout(self._heartbeat_seconds()):
                record = await store.load(run_id)

        except Exception:
            # Best effort — a read failure here must not turn a real deadline into an error.
            logger.warning(
                "Durable run %s could not be re-read to order its deadline against a "
                "possible cancel request; recording it as a timeout",
                run_id,
                exc_info=True,
            )

            return False

        return (
            record is not None
            and record.cancel_requested_at is not None
            and record.cancel_refused_at is None
        )

    # ....................... #

    async def _heartbeat(
        self,
        store: DurableRunStorePort,
        run_id: str,
        fence: int,
        body: asyncio.Future[JsonDict | None],
        reclaimed: asyncio.Event,
        cancel: DurableCancelSignal,
    ) -> None:
        seconds = self._heartbeat_seconds()
        refusal_written = False

        # Time until the *next* renewal. Normally one interval; shortened when extra work in
        # this loop has already eaten into it, so a renewal never drifts past the lease.
        delay = seconds

        while True:
            await asyncio.sleep(delay)
            delay = seconds

            try:
                # Bounded by the heartbeat interval: a renewal that cannot complete
                # within one interval has already failed its purpose, and left
                # unbounded a hung connection would wedge this loop silently while
                # the lease lapsed server-side — the exact double-execution window
                # the heartbeat exists to close.
                async with asyncio.timeout(seconds):
                    renewal = await store.renew(run_id, lease_for=self.lease_for, fence=fence)

            except Exception:
                # A renewal that errors (DB/network blip) or times out means we can no
                # longer prove we hold the lease; another worker may reclaim it. Treat it
                # as lease loss — stop the body before it double-executes and surface the
                # lease-loss path — rather than letting the raw error escape the heartbeat
                # task and override the body result. (``Exception`` leaves a genuine task
                # cancellation — ``CancelledError`` — to propagate so the ``finally``
                # cancel path still works.)
                logger.warning(
                    "Durable run %s heartbeat renewal errored; treating as lease loss",
                    run_id,
                    exc_info=True,
                )
                reclaimed.set()
                body.cancel()

                return

            if not renewal.held:
                # Another worker reclaimed the run; stop the body before it double-executes.
                reclaimed.set()
                body.cancel()

                return

            if renewal.cancel_requested and not cancel.requested:
                # An operator asked this run to stop. Flag it *before* the cancel so the body
                # (and the saga executor inside it) can classify the CancelledError it is
                # about to see, then tear the body down the same way lease loss and the
                # deadline do — cancel is the third consumer of one teardown path, not a
                # fourth mechanism.
                #
                # The loop deliberately keeps going. A saga past its pivot refuses the ask
                # and completes forward, which can take minutes; stopping renewal here would
                # let the lease lapse under a run that is still executing and hand a second
                # worker the same body.
                logger.info("Durable run %s was asked to stop; cancelling its body", run_id)
                cancel.request()
                body.cancel()

            if cancel.refused and not refusal_written:
                # Persist the refusal here rather than waiting for the run to finish. A saga
                # past its pivot refuses in memory and can then spend minutes completing
                # forward; if the worker dies in that window the row would carry an ask with
                # no refusal, and recovery would read it as an ordinary cancel-stamped run
                # and land it CANCELLED — abandoning a saga that had already committed at its
                # point of no return. Writing on the next beat shrinks that window from the
                # whole forward-completion to a single interval.
                #
                # Failure is not fatal: the write is retried on the next beat and the
                # runner's own ``finally`` is the backstop (the stamp is idempotent).
                #
                # Charged to the *sleep*, never to the lease. The renewal cadence has no
                # spare capacity: at ``heartbeat_divisor=2`` one sleep plus one renewal
                # already spend the whole lease, so a slow write left to run on top of that
                # would push the next renewal past expiry and hand a still-executing saga to
                # a second worker — the exact double-execution the heartbeat exists to
                # prevent, caused by the bookkeeping meant to make cancellation safer.
                started = perf_counter()

                try:
                    async with asyncio.timeout(seconds):
                        await store.refuse_cancel(run_id, fence=fence)

                    refusal_written = True

                except Exception:
                    logger.warning(
                        "Durable run %s could not record its cancel refusal on this "
                        "heartbeat; retrying on the next one",
                        run_id,
                        exc_info=True,
                    )

                delay = max(0.0, seconds - (perf_counter() - started))

    # ....................... #

    async def _expire_body_after(
        self,
        cap: timedelta,
        body: asyncio.Future[JsonDict | None],
        expired: asyncio.Event,
    ) -> None:
        await asyncio.sleep(cap.total_seconds())

        # Past the cap the body is no longer treated as live: cancel it (the same teardown
        # the heartbeat uses on lease loss) so the run frees its recovery slot instead of
        # renewing its lease forever.
        expired.set()
        body.cancel()

    # ....................... #

    def _mark_span_error(self, span: Span | None, error: BaseException) -> None:
        if self.telemetry is not None and span is not None:
            self.telemetry.mark_error(span, error)
