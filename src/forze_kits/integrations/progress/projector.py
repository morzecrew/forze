"""The projector — the one place the progress merge rules live.

Progress events arrive out of order. The tick lane is at-most-once and unordered by
contract; a resumed sweep re-reports counters it already reported; a durable transition
relayed after a crash lands behind ticks that were emitted after it. A projector that
simply wrote what it received would show a bar jumping backwards, a job resurrected after
it failed, and a "still running" row for work that finished twenty minutes ago.

So every event is merged against the stored row rather than applied to it, by two
independent rules:

- **``progress`` merges by max.** Never regresses, whatever the arrival order, and
  ``None`` (indeterminate) never overwrites a fraction anyone has already been shown.
  This is the second of the two monotonic layers — the reporter clamps at the source, and
  this defends whatever reaches the store, including from a source that never clamped.
- **Everything else follows one merge key**, ``(terminal, at, seq, rank)``, applied only
  when the incoming key is strictly greater. The terminal flag comes first, which is what
  makes a terminal status *absorbing*: it outranks every later-timestamped tick, so a
  straggler cannot resurrect a failed job, while two racing terminals still converge (the
  later one wins; at the same instant, ``failed`` outranks ``succeeded``).

Both rules are order-independent by construction, which is the property worth having: any
permutation of one job's events projects to the same record. Note where that puts
``message`` — the RFC's "always update on accepted events" would make the final message a
function of arrival order, so it rides the merge key too. The last message *by source
time* wins, which is what "the last human-readable line" means when the transport does not
preserve order.

``heartbeat_at`` is the exception, and takes the max of every accepted event: it answers
"when did we last hear from this job", so a reordered straggler must not make a live job
look stale.
"""

from collections.abc import Sequence
from datetime import datetime
from math import isfinite
from typing import Any, Final, final
from uuid import UUID

import attrs

from forze.application.contracts.document import DocumentCommandPort, DocumentQueryPort
from forze.application.contracts.realtime import RealtimeSignal
from forze.application.execution import ExecutionContext
from forze.base.exceptions import CoreException, ExceptionKind, exc
from forze_kits.integrations._logger import logger

from .events import JOB_PROGRESS_EVENT, JOB_PROGRESS_EVENT_NAME, JobProgress
from .record import (
    JobCreate,
    JobDocumentSpec,
    JobRecord,
    JobStatus,
    JobUpdate,
    job_record_spec,
)

# ----------------------- #

_MAX_MERGE_ATTEMPTS: Final = 8
"""Retry budget for one event's compare-and-merge loop.

Legitimate contention — a concurrent first event winning the insert, a second reporter
merging at the same moment — converges in a round or two. Exhausting the budget means the
row is unreachable rather than contended (a foreign row holding the job id outside this
scope, the tenancy-wiring failure the realtime cursors hit), and spinning on it would pin
a consumer forever."""

_STATUS_RANK: Final[dict[JobStatus, int]] = {
    JobStatus.PENDING: 0,
    JobStatus.RUNNING: 1,
    JobStatus.WAITING: 2,
    JobStatus.SUCCEEDED: 3,
    JobStatus.FAILED: 4,
}
"""Tie-break order for two statuses stamped at the same instant with the same sequence.

Only reachable across *different* reporters (one reporter's sequence always advances), so
this is the arbitration between two concurrent sources rather than a state machine: later
states outrank earlier ones, and a failure outranks a success — of two sources reporting
one job's end in the same instant, the one that saw an error saw more."""

_ACTIVE_STATUSES: Final = (JobStatus.RUNNING, JobStatus.WAITING)
"""The statuses a staleness sweep asks about — started, not finished."""


# ....................... #


def _merge_key(status: JobStatus, at: datetime, seq: int) -> tuple[int, datetime, int, int]:
    """The total order events are merged by (see the module docstring)."""

    return (int(status.is_terminal), at, seq, _STATUS_RANK[status])


# ....................... #


def _clean_progress(value: float | None, *, job_id: UUID) -> float | None:
    """Clamp a reported fraction into ``0.0..1.0``; refuse the values that break ``max``.

    Observability must not break the work it observes, so an out-of-range fraction is
    clamped and reported rather than raised: a caller whose counter overshoots by one row
    should not lose a six-hour export. ``NaN`` is the exception that cannot be clamped —
    every comparison against it is false, so one would silently poison the max-merge into
    accepting anything afterwards. It is dropped to indeterminate instead.
    """

    if value is None:
        return None

    if not isfinite(value):
        logger.warning(
            "Dropping a non-finite progress value; treating the job as indeterminate",
            job_id=str(job_id),
            value=repr(value),
        )

        return None

    return min(1.0, max(0.0, float(value)))


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class JobProgressProjector:
    """Merge progress events into the job read model.

    Run it **inline** (the reporter's record sink) where the work and the store share a
    process, or as an **inbox consumer** where they do not — the merge is the same either
    way, which is the point of it living in one place. Built via
    :func:`build_job_progress_projector`.
    """

    command: DocumentCommandPort[JobRecord, Any, JobCreate, JobUpdate]
    """The document command port for creating and merging job rows."""

    query: DocumentQueryPort[JobRecord]
    """The document query port for reading job rows."""

    # ....................... #

    async def apply(self, event: JobProgress) -> JobRecord | None:
        """Merge *event* into its job's row; return the row, or ``None`` if it was dropped.

        ``None`` means the event changed nothing — the only case is a non-terminal report
        for a job that has already finished, which is dropped whole (it does not even
        refresh the heartbeat: a finished job is not stuck, and its last-heard-from time
        belongs to the run that finished it).
        """

        for _ in range(_MAX_MERGE_ATTEMPTS):
            row = await self.query.find({"$values": {"id": event.job_id}})

            if row is None:
                created = await self._create(event)

                if created is not None:
                    return created

                continue  # a concurrent first event won the insert — merge onto it

            update = _merged_update(row, event)

            if update is None:
                return row

            try:
                return await self.command.update(row.id, row.rev, update)

            except CoreException as error:
                if error.kind is ExceptionKind.CONCURRENCY:
                    continue  # a concurrent merge bumped the rev — re-read and retry

                raise

        raise exc.internal(
            f"Job {event.job_id} did not converge after {_MAX_MERGE_ATTEMPTS} merge "
            "attempts: the row is invisible to this scope yet its id conflicts on insert "
            "(a foreign row is holding the job id — check the collection's tenancy wiring).",
            code="job_progress_merge_stalled",
        )

    # ....................... #

    async def apply_signal(self, signal: RealtimeSignal) -> JobRecord | None:
        """Merge a realtime signal, ignoring anything that is not ``job.progress``.

        The consumer-side entry point: a gateway or inbox consumer hands over whatever the
        realtime stream carried, and a signal for some other declared event is not an error
        here — one stream carries the whole egress surface.
        """

        if signal.event != JOB_PROGRESS_EVENT_NAME:
            return None

        return await self.apply(JOB_PROGRESS_EVENT.parse(signal.payload))

    # ....................... #

    async def find_stalled(
        self,
        *,
        silent_since: datetime,
        kind: str | None = None,
        limit: int = 100,
    ) -> Sequence[JobRecord]:
        """Jobs that started, have not finished, and have not reported since *silent_since*.

        The "is anything stuck?" query, answered by an index rather than by reading
        payloads: it filters on ``status`` and ``heartbeat_at`` alone (both plaintext by
        :func:`~.record.job_record_spec`'s own rule) and returns the quietest first, so a
        dashboard or an alarm can page a fixed number of the worst offenders whatever the
        collection's size.
        """

        values: dict[str, Any] = {
            "status": {"$in": [status.value for status in _ACTIVE_STATUSES]},
            "heartbeat_at": {"$lt": silent_since},
        }

        if kind is not None:
            values["kind"] = kind

        page = await self.query.find_many(
            filters={"$values": values},
            sorts={"heartbeat_at": "asc"},
            pagination={"limit": limit},
        )

        return page.hits

    # ....................... #

    async def _create(self, event: JobProgress) -> JobRecord | None:
        """Insert the row for a job nobody has seen; ``None`` when a concurrent event won.

        A late joiner is normal, not an error: a dashboard started mid-sweep, a consumer
        replaying from an offset, an inbox that got the ticks before the transition. It
        gets a row and a warning rather than an exception, because refusing the event would
        lose the only record of work that is genuinely running.

        The warning is for a row created *mid-story* — an event that is neither the job's
        declaration (``pending``) nor its very first report. A reporter that opens a job by
        starting it is the ordinary path and must stay silent, or every job in the system
        would log a warning about itself and the signal would be worth nothing.
        """

        if event.status is not JobStatus.PENDING and event.seq > 1:
            logger.warning(
                "Creating a job record from a non-pending progress event (a late joiner)",
                job_id=str(event.job_id),
                kind=event.kind,
                status=event.status.value,
            )

        started = event.at if event.status is not JobStatus.PENDING else None

        try:
            return await self.command.create(
                JobCreate(
                    kind=event.kind,
                    status=event.status,
                    progress=_clean_progress(event.progress, job_id=event.job_id),
                    message=event.message,
                    subject=event.subject,
                    durable_run_id=event.durable_run_id,
                    error=event.error,
                    heartbeat_at=event.at,
                    started_at=started,
                    finished_at=event.at if event.status.is_terminal else None,
                    event_at=event.at,
                    event_seq=event.seq,
                ),
                id=event.job_id,
            )

        except CoreException as error:
            if error.kind is ExceptionKind.CONFLICT:
                return None

            raise


# ....................... #


def _merged_update(row: JobRecord, event: JobProgress) -> JobUpdate | None:
    """The update *event* implies for *row*, or ``None`` when it implies nothing.

    Pure and side-effect free on purpose — this is the rule set, and it is worth being able
    to test it as a function of ``(row, event)`` rather than only through a store.
    """

    # A terminal record absorbs: a tick that was still in flight when the job ended says
    # nothing about a job that has ended — with one exception, and it is the exception that
    # makes the projection order-independent. Absorption governs the job's *state*; it
    # cannot govern a fact about the past that can only ever be learned. If the terminal
    # transition is the first event to arrive (a consumer replaying from an offset, a
    # relay catching up after a crash), the row is created believing the job started when
    # it ended, and every event that would have corrected that is a straggler. Letting a
    # straggler lower `started_at` — and nothing else — is what makes "finished at 12:05"
    # and "ran for four hours" both true whatever order they were learned in.
    if row.status.is_terminal and not event.status.is_terminal:
        if event.status is not JobStatus.PENDING and (
            row.started_at is None or event.at < row.started_at
        ):
            return JobUpdate(started_at=event.at)

        return None

    # Only the keys set here reach the store (update payloads are applied by *set* field,
    # not by non-``None`` value), which is what lets the merge clear ``message``/``error``
    # deliberately while leaving every field it did not decide on untouched.
    patch: dict[str, Any] = {}

    progress = _clean_progress(event.progress, job_id=event.job_id)

    if progress is not None and (row.progress is None or progress > row.progress):
        patch["progress"] = progress

    if event.at > row.heartbeat_at:
        patch["heartbeat_at"] = event.at

    # The earliest report that says the job left `pending` is when it started — a min, so
    # that a late-arriving first tick still dates the start correctly.
    if event.status is not JobStatus.PENDING and (
        row.started_at is None or event.at < row.started_at
    ):
        patch["started_at"] = event.at

    if _merge_key(event.status, event.at, event.seq) > _merge_key(
        row.status, row.event_at, row.event_seq
    ):
        patch["status"] = event.status
        patch["message"] = event.message
        patch["error"] = event.error
        patch["event_at"] = event.at
        patch["event_seq"] = event.seq

        # Identity-ish fields only ever fill in: a tick that omits the subject or the run
        # it came from is not asserting that the job has neither.
        if event.subject is not None:
            patch["subject"] = event.subject

        if event.durable_run_id is not None:
            patch["durable_run_id"] = event.durable_run_id

        if event.status.is_terminal:
            patch["finished_at"] = event.at

    return JobUpdate(**patch) if patch else None


# ....................... #


def build_job_progress_projector(
    ctx: ExecutionContext,
    *,
    spec: JobDocumentSpec | None = None,
) -> JobProgressProjector:
    """Resolve the job collection's document ports once and build the projector.

    The publisher pattern: a misrouted spec fails where the projector is built, not on the
    first event. Refuses a build in a read-only (``QUERY``) operation — projecting writes.
    """

    if ctx.inv_ctx.is_read_only():
        raise exc.precondition(
            "Cannot build a JobProgressProjector in a read-only (QUERY) operation"
        )

    resolved = spec if spec is not None else job_record_spec()

    return JobProgressProjector(
        command=ctx.document.command(resolved),
        query=ctx.document.query(resolved),
    )
