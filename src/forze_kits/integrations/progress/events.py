"""The one declared progress event — ``job.progress``.

Per the frozen-catalog doctrine, realtime events are *declared*, never minted per block of
work: there is no ``export.progress`` and no ``reencrypt.progress``, only this one event
whose payload carries the job's :attr:`~.record.JobRecord.kind`. Consumers filter on the
payload, and the realtime egress surface stays enumerable — a per-kind event name would
make it a function of how many kinds of work an application happens to run.

The payload is also the projector's input, so it carries everything the record needs to be
*created* from a late-joining event, not just the fields a UI paints: an operator who
starts a dashboard mid-sweep gets a row, not a warning about an id nobody has seen.
"""

from typing import Final
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, model_validator

from forze.application.contracts.realtime import RealtimeEvent

from .record import JobStatus

# ----------------------- #

JOB_PROGRESS_EVENT_NAME: Final[str] = "job.progress"
"""Wire name of the one declared progress event."""


# ....................... #


class JobProgress(BaseModel):
    """One progress report: where a job is, as of one instant at one source."""

    model_config = ConfigDict(frozen=True)

    job_id: UUID
    """The job this report is about — also the record's primary key."""

    kind: str
    """The job's app-defined kind, so a consumer can filter one declared event."""

    status: JobStatus
    """The job's status as the reporter sees it."""

    at: AwareDatetime
    """When the reporter produced this event (its own clock, via the time seam).

    The merge orders by this rather than by arrival: the transport for ticks is
    at-most-once and unordered, and resumed work re-reports from wherever it restarted, so
    arrival order carries no information about what is newer.

    **Aware, and refused if not.** This instant is compared against the record's stored ones
    on every merge, and Python raises on a naive-vs-aware comparison — so a producer that
    serialized a local timestamp without an offset (another language, a hand-built payload)
    would not merge wrong, it would raise ``TypeError`` out of the middle of the projector
    and leave the job unprojected. Refusing it here makes that a validation error at the
    boundary, where the payload is still identifiable.
    """

    seq: int = 0
    """The reporter's own emission counter — the tie-break within one instant.

    Only meaningful next to :attr:`at`, and only within one reporter: a fresh reporter
    (a new run resuming the same job) restarts at zero, which is harmless because its
    events carry a later :attr:`at`.
    """

    progress: float | None = None
    """Fraction complete in ``0.0..1.0``; ``None`` means indeterminate."""

    message: str | None = None
    """Human-readable line for this report."""

    subject: str | None = None
    """What the job is about (carried so a late joiner's row is complete)."""

    durable_run_id: str | None = None
    """The durable run that produced this report, when there is one."""

    error: str | None = None
    """Why the job failed — set only on a :attr:`JobStatus.FAILED` report."""

    # ....................... #

    @model_validator(mode="after")
    def _error_belongs_to_a_failure(self) -> "JobProgress":
        """Keep "set only on a failed report" a rule rather than a comment.

        The projector copies ``error`` onto the record with every status it accepts, so a
        report that carries one without failing produces a row that says ``running`` and
        explains why it failed — and, once the job succeeds, a success with a reason. The
        record cannot repair that (the reason is genuine input, and dropping it silently
        would lose the only evidence of a broken producer), so the payload refuses it.
        """

        if self.error is not None and self.status is not JobStatus.FAILED:
            raise ValueError(
                f"A progress report for job {self.job_id} carries an error but reports "
                f"{self.status.value!r}; error belongs to a failed report only."
            )

        return self


# ....................... #

JOB_PROGRESS_EVENT: Final[RealtimeEvent[JobProgress]] = RealtimeEvent(
    name=JOB_PROGRESS_EVENT_NAME,
    payload_type=JobProgress,
)
"""The declared event. Register it in the application's :class:`RealtimeEventCatalog`.

:attr:`~forze.application.contracts.realtime.RealtimeEvent.offline_delivery` stays on
(the default): the signals that reach a principal's mailbox are the *durable* ones, and
those are exactly the status transitions — "your export is ready" is worth delivering to
someone who was offline when it finished. Ticks never get there; they ride the ephemeral
lane and are dropped by construction, which is the split doing the filtering rather than
this flag.
"""
