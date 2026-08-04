"""``ProgressReporter`` — the write side a long-running operation holds.

A handler, a durable body, or an external worker builds one of these and calls it as the
work advances. Two things are built in, because both are rules a caller should not have to
rediscover:

- **Coalescing.** A row-by-row loop reports thousands of times a second, and a realtime
  lane that carries every one of them is a self-inflicted firehose — the transport is not
  the problem to solve, the emission rate is. Reports inside the throttle window are kept
  (last value wins) rather than dropped, and the pending value is flushed by the next
  eligible report, by any status transition, by reaching ``1.0``, and by
  :meth:`ProgressReporter.flush`. So the *rate* is bounded but the *final value* never
  goes missing, which is the part a plain rate limiter gets wrong.
- **Monotonicity at the source.** Resumed work re-reports counters it already reported, so
  a fraction below the high-water mark is clamped up rather than published as a bar
  jumping backwards. The projector clamps again on the other side: two independent layers,
  because the source that skips this one is exactly the source nobody controls (an
  external worker, a future adapter).

Transitions and ticks take different lanes, which is the whole reason the reporter needs a
sink abstraction rather than a publish call: a lost tick costs nothing (the next one
carries a newer value), while a lost terminal transition costs an eternally-running job on
every dashboard. Ticks go out ephemeral and are allowed to fail; transitions go out
durable and are not.
"""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from math import isfinite
from typing import Final, Protocol, final, runtime_checkable
from uuid import UUID

import attrs

from forze.application.contracts.durable.function import current_durable_run
from forze.application.contracts.outbox import (
    OutboxCommandPort,
    OutboxDestination,
    OutboxSpec,
    OutboxStagingContext,
)
from forze.application.contracts.realtime import Audience, RealtimeSignal
from forze.application.contracts.stream import StreamSpec
from forze.application.execution import ExecutionContext
from forze.base.exceptions import CoreException, exc
from forze.base.primitives import monotonic, utcnow
from forze_kits.integrations._logger import logger
from forze_kits.integrations.realtime import (
    DEFAULT_REALTIME_CHANNEL,
    RealtimePublisher,
    build_realtime_publisher,
    realtime_outbox_spec,
)

from .events import JOB_PROGRESS_EVENT, JobProgress
from .record import JobStatus

# ----------------------- #

DEFAULT_MIN_INTERVAL: Final[float] = 0.3
"""Default coalescing window in seconds — ~3 ticks a second reach the transport."""

DEFAULT_PROGRESS_ROUTE: Final[str] = "job-progress"
"""Default outbox route for progress transitions — dedicated, never the app's own."""


# ....................... #


@runtime_checkable
class ProgressSink(Protocol):
    """Where a reporter's accepted events go.

    *durable* says which lane the event belongs to, not which one the sink must use: a
    sink with only one lane (an inline projector writing the record) applies both the same
    way. It is the transport-backed sinks that owe the distinction.
    """

    async def emit(self, event: JobProgress, *, durable: bool) -> None:
        """Deliver *event*."""
        ...  # pragma: no cover


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class RealtimeProgressSink:
    """Publish progress onto the realtime plane — ticks ephemeral, transitions durable.

    Adopts the publisher's existing split rather than extending it: ``publish`` for ticks
    (at-most-once, fire-and-forget) and ``stage`` for status transitions (outbox-durable,
    relayed after commit).

    Each transition is staged **and flushed**, which is why :attr:`route` has to be a route
    of this plane's own. Staging buffers per task and a flush closes the route for the rest
    of that task, so a job that reports ``running`` and later ``succeeded`` from one task —
    every long sweep — could otherwise stage exactly once. Re-opening the flag before each
    stage is the sanctioned move on a *dedicated* route (the search-sync marker route does
    the same); on a shared route it would flush somebody else's staged rows early, so
    :func:`build_progress_reporter` refuses a route it can tell is shared.

    Waiting for the caller's unit of work to flush is not an option here: a job can sit in
    ``waiting`` across the end of the run that paused it (the terminate-and-resume grain),
    and a transition still buffered when that run ends is a transition nobody ever sees.
    """

    publisher: RealtimePublisher
    """The resolved realtime publisher."""

    audience: Audience
    """Who the signals are addressed to (a topic per job, or the requesting principal)."""

    outbox: OutboxCommandPort[RealtimeSignal]
    """The staged route's command port — flushed after every transition."""

    staging: OutboxStagingContext
    """The per-task staging state whose flushed flag this re-opens."""

    route: str
    """The dedicated outbox route carrying progress transitions."""

    # ....................... #

    async def emit(self, event: JobProgress, *, durable: bool) -> None:
        if not durable:
            await self.publisher.publish(self.audience, JOB_PROGRESS_EVENT, event)

            return

        self.staging.set_flushed(self.route, False)
        await self.publisher.stage(self.audience, JOB_PROGRESS_EVENT, event)
        await self.outbox.flush()


# ....................... #


@runtime_checkable
class JobProgressMerger(Protocol):
    """The projector, as the reporter needs it (structural, so the kit stays composable)."""

    async def apply(self, event: JobProgress) -> object:
        """Merge *event* into the job read model."""
        ...  # pragma: no cover


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class RecordProgressSink:
    """Write progress into the job read model inline, through the projector.

    For work whose store is in the same process as the work itself — the common case for a
    sweep or an export. **Report from outside the unit of work**: this writes through the
    caller's ports, so a ``fail()`` recorded inside a transaction that then rolls back
    takes the failure record with it, and the job stays "running" forever. Long sweeps are
    not one transaction, which is why this is the default; work that *is* transactional
    should publish and let a consumer-side projector own the record.
    """

    projector: JobProgressMerger
    """The projector that owns the merge rules."""

    # ....................... #

    async def emit(self, event: JobProgress, *, durable: bool) -> None:
        await self.projector.apply(event)


# ....................... #


@final
@attrs.define(slots=True, kw_only=True)  # not frozen — the throttle and high-water mark
class ProgressReporter:
    """Report a long-running operation's progress. Built via :func:`build_progress_reporter`.

    One reporter is one job's write side for the duration of one run. It is not safe to
    share across concurrent tasks reporting different work — the throttle, the sequence and
    the high-water mark are per-job state — but two reporters for the *same* job in
    different processes are fine, which is what a resumed task needs (the projector merges
    them).
    """

    job_id: UUID
    """The job being reported on — the record's primary key."""

    kind: str
    """The job's app-defined kind, carried on every event."""

    sinks: tuple[ProgressSink, ...]
    """Where events go, in order. Empty is refused at build (a reporter nobody hears)."""

    subject: str | None = None
    """What the job is about, carried so a late-joining consumer's row is complete."""

    durable_run_id: str | None = None
    """The durable run carrying this job, when there is one (auto-filled at build)."""

    min_interval: float = DEFAULT_MIN_INTERVAL
    """Coalescing window in seconds; ``0`` emits every report."""

    # ....................... #

    _status: JobStatus = attrs.field(default=JobStatus.PENDING, init=False)
    _progress: float | None = attrs.field(default=None, init=False)
    _seq: int = attrs.field(default=0, init=False)
    _last_emit: float | None = attrs.field(default=None, init=False)
    _pending: JobProgress | None = attrs.field(default=None, init=False)

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if not self.sinks:
            raise exc.configuration(
                "A ProgressReporter needs at least one sink: one with none reports into "
                "nothing, which reads exactly like work that never reported.",
                code="progress_reporter_no_sinks",
            )

        if self.min_interval < 0:
            raise exc.configuration("ProgressReporter min_interval cannot be negative")

    # ....................... #

    @property
    def status(self) -> JobStatus:
        """The status this reporter last reported."""

        return self._status

    # ....................... #

    @property
    def progress(self) -> float | None:
        """The high-water mark this reporter has reported."""

        return self._progress

    # ....................... #

    async def start(self, message: str | None = None) -> None:
        """Report that the work is executing — also the resume transition out of ``waiting``."""

        await self._transition(JobStatus.RUNNING, message=message)

    # ....................... #

    async def report(self, progress: float | None, message: str | None = None) -> None:
        """Report a fraction complete (``None`` = indeterminate), coalesced.

        Clamped up to the high-water mark and into ``0.0..1.0``; a non-finite value is
        refused outright, because a ``NaN`` high-water mark makes every later comparison
        false and silently disables the monotonic clamp for the rest of the run.
        """

        before = self._progress
        self._advance_high_water(progress)

        # A bar that has just reached the end always escapes the window: it may be the last
        # thing this job ever says, and a coalesced 1.0 that nothing flushes is a job stuck
        # at 99% forever. Only the *transition* to 1.0 forces the emit, so a loop that keeps
        # reporting 1.0 stays coalesced.
        await self._tick(message, force=self._progress == 1.0 and before != 1.0)

    # ....................... #

    async def advance(self, done: int, total: int, message: str | None = None) -> None:
        """Report ``done`` of ``total`` — the counting form of :meth:`report`.

        A non-positive *total* is indeterminate rather than an error: "0 of 0" is what a
        sweep legitimately knows before it has counted, and a division is not worth
        failing a job over.
        """

        fraction = done / total if total > 0 else None

        await self.report(fraction, message)

    # ....................... #

    async def heartbeat(self, message: str | None = None) -> None:
        """Report liveness without moving the bar (coalesced like any tick)."""

        await self._tick(message)

    # ....................... #

    async def wait(self, message: str | None = None) -> None:
        """Report that the work is paused on something external (non-terminal, reversible)."""

        await self._transition(JobStatus.WAITING, message=message)

    # ....................... #

    async def finish(self, message: str | None = None) -> None:
        """Report success. Completes the bar: a finished job is ``1.0``, never ``0.97``."""

        self._progress = 1.0
        await self._transition(JobStatus.SUCCEEDED, message=message)

    # ....................... #

    async def fail(self, error: str, message: str | None = None) -> None:
        """Report failure. The bar keeps its last value — a failed job did not complete."""

        await self._transition(JobStatus.FAILED, message=message, error=error)

    # ....................... #

    @asynccontextmanager
    async def track(self, message: str | None = None) -> AsyncIterator["ProgressReporter"]:
        """Run a block as this job: ``running`` on entry, and an outcome on exit — unless the
        block already stated one.

        The lifecycle is the part callers forget, and forgetting it is not a missing
        message — it is a job that shows as running on every dashboard for the rest of the
        deployment's life, because the only thing that could have said otherwise was an
        exception on its way up. The failure is recorded and then re-raised: the job's
        record is an observation, never a handler of the error.

        **It closes a job it opened, never one the block already spoke for.** Two cases,
        and both are ordinary rather than exotic:

        - the block **paused** the job (:meth:`wait`) — a terminate-and-resume run ends
          cleanly *because* it succeeded at producing a question, and finishing the job here
          would report the task as completed, which is precisely the lie ``waiting`` exists
          to prevent;
        - the block **finished or failed** the job itself — a second transition would raise
          ``progress_job_terminal`` out of the ``async with``, punishing the caller who was
          most explicit.

        An exception still records a failure even from ``waiting`` (a run that died on its
        way to handing off did not hand off), which is the RFC's rule that terminal beats a
        pause. A block that already reached terminal keeps its own word.
        """

        await self.start(message)

        try:
            yield self

        except BaseException as error:
            if self._status.is_terminal:
                if self._status is JobStatus.SUCCEEDED:
                    # Worth saying out loud: the job claims success and the block raised
                    # anyway. Both facts are real, and only one of them is in the record.
                    logger.warning(
                        "A tracked block raised after reporting success; the job keeps the "
                        "outcome it stated",
                        job_id=str(self.job_id),
                        kind=self.kind,
                        error=f"{type(error).__name__}: {error}",
                    )

                raise

            # `BaseException`: a cancelled sweep (deploy, deadline, shutdown) is exactly
            # the case where a job otherwise stays "running" forever, and cancellation is
            # not a `CoreException`.
            try:
                await self.fail(f"{type(error).__name__}: {error}")

            except BaseException as recording_error:
                # Recording the failure is best-effort and must never replace the failure
                # it was recording: under cancellation the write itself is cancelled at its
                # first await, and letting that escape would hand the caller a
                # `CancelledError` where their own exception should have been.
                logger.warning(
                    "Could not record a job failure; re-raising the original error",
                    job_id=str(self.job_id),
                    kind=self.kind,
                    error=str(recording_error),
                )

            raise

        if self._status is JobStatus.RUNNING:
            await self.finish()

    # ....................... #

    async def flush(self) -> None:
        """Emit the coalesced report still waiting in the window, if any."""

        pending = self._pending

        if pending is None:
            return

        self._pending = None
        self._last_emit = monotonic()
        await self._emit(pending, durable=False)

    # ....................... #

    def _advance_high_water(self, progress: float | None) -> None:
        """Clamp *progress* into range and up to the high-water mark (layer one of two)."""

        if progress is None:
            return

        if not isfinite(progress):
            raise exc.validation(
                f"Progress for job {self.job_id} must be a finite fraction, got {progress!r}",
                code="progress_not_finite",
            )

        clamped = min(1.0, max(0.0, float(progress)))

        if self._progress is None or clamped > self._progress:
            self._progress = clamped

    # ....................... #

    def _event(self, *, message: str | None, error: str | None = None) -> JobProgress:
        """Stamp the next event. The sequence is what orders a burst the clock cannot."""

        self._seq += 1

        return JobProgress(
            job_id=self.job_id,
            kind=self.kind,
            status=self._status,
            at=utcnow(),
            seq=self._seq,
            progress=self._progress,
            message=message,
            subject=self.subject,
            durable_run_id=self.durable_run_id,
            error=error,
        )

    # ....................... #

    async def _tick(self, message: str | None, *, force: bool = False) -> None:
        """Emit a non-transition report, or hold it as the pending one for this window.

        Holding rather than dropping is what makes the throttle a *coalescer*: the window
        bounds how often the transport hears from a job, never which value it ends on.
        """

        if self._status.is_terminal:
            raise exc.precondition(
                f"Job {self.job_id} already reported {self._status.value}; a terminal job "
                "cannot report progress again (the projector would drop the event anyway).",
                code="progress_job_terminal",
            )

        event = self._event(message=message)
        now = monotonic()

        if not force and self._within_window(now):
            self._pending = event

            return

        self._pending = None
        self._last_emit = now
        await self._emit(event, durable=False)

    # ....................... #

    def _within_window(self, now: float) -> bool:
        """Whether the coalescing window opened by the last emit is still open."""

        if self.min_interval <= 0 or self._last_emit is None:
            return False

        return now - self._last_emit < self.min_interval

    # ....................... #

    async def _transition(
        self,
        status: JobStatus,
        *,
        message: str | None,
        error: str | None = None,
    ) -> None:
        """Move to *status* and emit it durably, flushing whatever the window was holding."""

        if self._status.is_terminal:
            raise exc.precondition(
                f"Job {self.job_id} already reported {self._status.value}; it cannot also "
                f"report {status.value}.",
                code="progress_job_terminal",
            )

        # The held tick first, so its message is not overwritten out of order by the
        # transition that follows it in the same instant.
        await self.flush()

        self._status = status
        self._last_emit = monotonic()
        await self._emit(self._event(message=message, error=error), durable=True)

    # ....................... #

    async def _emit(self, event: JobProgress, *, durable: bool) -> None:
        """Fan out to every sink — transitions propagate failures, ticks absorb them.

        The asymmetry is the transport contract, applied one layer up: the tick lane is
        at-most-once by definition, so a sink that cannot take one has lost nothing anyone
        was promised, and killing a six-hour export over an unreachable dashboard would be
        the worse outcome. A transition is the event whose loss leaves a job wrong forever,
        so it is allowed to fail the caller.
        """

        for sink in self.sinks:
            try:
                await sink.emit(event, durable=durable)

            except CoreException as error:
                if durable:
                    raise

                logger.warning(
                    "Dropping a progress tick: its sink refused it",
                    job_id=str(self.job_id),
                    kind=self.kind,
                    sink=type(sink).__name__,
                    error=str(error),
                )

            except Exception:
                # Anything a sink raises that is *not* a framework exception is a defect in
                # that sink — an unmapped client error, a bug in a custom one. It is still
                # not worth a six-hour export, so a tick absorbs it too; logged with its
                # traceback rather than a one-liner, because unlike a refused transport this
                # is nobody's expected failure mode. `BaseException` stays uncaught:
                # cancellation is the runtime asking this task to stop, not a sink failing.
                if durable:
                    raise

                logger.exception(
                    "A progress sink raised on a tick; dropping it",
                    job_id=str(self.job_id),
                    kind=self.kind,
                    sink=type(sink).__name__,
                )


# ....................... #


def progress_outbox_spec(
    name: str = DEFAULT_PROGRESS_ROUTE,
    *,
    stream: str = DEFAULT_REALTIME_CHANNEL,
    queue: str | None = None,
) -> OutboxSpec[RealtimeSignal]:
    """The outbox route progress transitions are staged on, relayed to *stream*.

    A route of its own, relaying into the ordinary realtime stream — so consumers see one
    channel while progress keeps a staging lane it can flush after every transition (see
    :class:`RealtimeProgressSink`). Register a relay for it alongside the realtime one::

        realtime_relay_lifecycle_step(
            outbox_spec=progress_outbox_spec(), stream_spec=transport.stream_spec
        )

    Pass *queue* to relay transitions to a **queue** instead, which is the out-of-process
    projector's shape: a consumer runner drains it and applies each signal to the record.
    That is a fork, not a tweak — transitions then reach the projector and *not* the stream,
    so a live UI on the stream sees ticks only and must read the record for status. Choose
    it when the record is what the UI reads; keep the stream when the UI follows signals.
    """

    if queue is None:
        return realtime_outbox_spec(name, stream=stream)

    return attrs.evolve(
        realtime_outbox_spec(name, stream=stream),
        destination=OutboxDestination.queue(route=queue, channel=queue),
    )


# ....................... #


def job_topic(job_id: UUID) -> Audience:
    """The default audience for a job's signals — one topic per job.

    A topic rather than a principal because a job is watched by whoever opened the page:
    the operator who started it, a colleague, a dashboard. Addressing the *starter* would
    make progress invisible to everyone else and would put ticks in their offline mailbox.

    **A topic is a room, and the framework does not decide who may join one** — the gateway
    does, from the connection's identity, exactly as for every other topic. So a job's
    ``message`` and ``subject`` are visible to whoever your gateway lets into
    ``job:<id>``. The id is unguessable, which is not the same as authorized: for work whose
    progress text carries anything a bystander should not read, either keep the detail out
    of ``message`` or address a principal instead.
    """

    return Audience.topic(f"job:{job_id}")


# ....................... #


def build_progress_reporter(
    ctx: ExecutionContext,
    *,
    job_id: UUID,
    kind: str,
    subject: str | None = None,
    audience: Audience | None = None,
    stream_spec: StreamSpec[RealtimeSignal] | None = None,
    outbox_spec: OutboxSpec[RealtimeSignal] | None = None,
    projector: JobProgressMerger | None = None,
    min_interval: float = DEFAULT_MIN_INTERVAL,
) -> ProgressReporter:
    """Build a reporter wired to the realtime plane, the job record, or both.

    Pass *stream_spec* + *outbox_spec* for the realtime lane, *projector* for the record,
    or both (the usual shape: the record is what a page load reads, the signals are what
    keeps it moving without polling). At least one is required — see
    :class:`ProgressReporter`.

    ``durable_run_id`` fills itself in from the durable run bound to this task, so a job
    reported from inside a durable body links to its run without the caller threading an
    id it did not ask for. Outside a run it stays ``None``.
    """

    sinks: list[ProgressSink] = []

    if projector is not None:
        sinks.append(RecordProgressSink(projector=projector))

    if stream_spec is not None:
        if outbox_spec is None:
            # Ticks would still flow, so this fails at build rather than at the one call
            # that matters: the job would run, publish, and then be unable to say it
            # finished — the exact "eternally running on every dashboard" outcome the
            # durable lane exists to prevent.
            raise exc.configuration(
                "A realtime progress reporter needs an outbox route as well as a stream: "
                "status transitions are staged durably (a lost terminal transition leaves "
                "a finished job showing as running forever), and only ticks are ephemeral. "
                "Pass progress_outbox_spec().",
                code="progress_reporter_no_outbox",
            )

        if str(outbox_spec.name) == str(stream_spec.name):
            # The realtime transport bundle names its outbox route after its channel, so
            # this is the shape of somebody handing over `build_realtime_transport()`'s
            # pair. Progress flushes its route after every transition, which on a route the
            # application also stages business signals through would persist those signals
            # early — outside the transaction that was still deciding whether to keep them.
            raise exc.configuration(
                f"Progress transitions need an outbox route of their own; {outbox_spec.name!r} "
                "is the realtime channel's shared route. Progress flushes after every "
                "transition (a job can pause across the end of its run, so a buffered "
                "transition would never be seen), and flushing a shared route persists "
                "whatever else was staged on it. Pass progress_outbox_spec() and relay it "
                "to the same stream.",
                code="progress_reporter_shared_outbox",
            )

        sinks.append(
            RealtimeProgressSink(
                publisher=build_realtime_publisher(
                    ctx, stream_spec=stream_spec, outbox_spec=outbox_spec
                ),
                audience=audience if audience is not None else job_topic(job_id),
                outbox=ctx.outbox.command(outbox_spec),
                staging=ctx.outbox_staging,
                route=str(outbox_spec.name),
            )
        )

    run = current_durable_run()

    return ProgressReporter(
        job_id=job_id,
        kind=kind,
        sinks=tuple(sinks),
        subject=subject,
        durable_run_id=run.run_id if run is not None else None,
        min_interval=min_interval,
    )
