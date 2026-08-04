"""The job read model — one row per long-running operation, and its spec factory.

A job record *observes* work; it never controls it. The row is keyed by the **job id**
(the document's own primary key), which is what lets the record outlive the thing doing
the work: one logical task may be carried by many durable runs over its life (a
terminate-and-resume agent task pauses between runs), and across those runs the job is
continuous — ``progress`` is the task's fraction, :attr:`JobStatus.WAITING` is the pause,
and :attr:`JobDoc.durable_run_id` names whichever run is carrying it right now.

The collection is the framework's, the table is the application's (the
``realtime_mailbox_spec`` pattern): :func:`job_record_spec` hands over the spec and the DDL
is documented rather than migrated for you. Wire it **tenant-aware** and the adapter injects
and scopes ``tenant_id`` like every other collection — the models carry no tenant field of
their own. Of the models below only :class:`JobRecord` is meant to be read by application
code; the write models exist because the projector has to construct them, not as a surface
anyone else writes through.
"""

from datetime import datetime
from enum import StrEnum
from typing import Final, final

from pydantic import Field

from forze.application.contracts.crypto import FieldEncryption
from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.base.exceptions import exc
from forze.base.primitives import utcnow
from forze.domain.models import BaseDTO, Document, ReadDocument

# ----------------------- #

DEFAULT_JOB_COLLECTION: Final[str] = "jobs"
"""Default collection name for the job read model."""


# ....................... #


@final
class JobStatus(StrEnum):
    """Where a job is — an observability vocabulary, deliberately not a run's.

    A durable run's status is an execution claim ("this body is executing, this worker
    holds the lease"); a job's status is what an operator or an end user should be told.
    The two are linked by id and never unified: one job spans many runs, and a run that
    *succeeded at producing a question* leaves its job :attr:`WAITING`, not
    :attr:`SUCCEEDED`.
    """

    PENDING = "pending"
    """Created, nothing has started it yet."""

    RUNNING = "running"
    """Something is executing the work right now."""

    WAITING = "waiting"
    """Started, but not executing: paused on something external.

    A human answer, an upstream job, an approval. Distinct from :attr:`RUNNING` (nobody is
    executing) and from terminal (it will continue) — this is the observable state of a
    terminate-and-resume task *between* rounds, where the durable run that produced the
    question genuinely completed and only the job is still open. Non-terminal and
    reversible: resuming moves it back to :attr:`RUNNING`.
    """

    SUCCEEDED = "succeeded"
    """Terminal: the work completed."""

    FAILED = "failed"
    """Terminal: the work stopped on an error (see ``error``)."""

    # ....................... #

    @property
    def is_terminal(self) -> bool:
        """Whether this status absorbs — nothing moves a job out of it."""

        return self in _TERMINAL


_TERMINAL: Final = frozenset({JobStatus.SUCCEEDED, JobStatus.FAILED})


# ....................... #
# document models (NO tenant_id — the tenant-aware adapter injects + scopes it)


class JobDoc(Document):
    kind: str
    status: JobStatus = JobStatus.PENDING
    progress: float | None = None
    message: str | None = None
    subject: str | None = None
    durable_run_id: str | None = None
    error: str | None = None
    heartbeat_at: datetime = Field(default_factory=utcnow)
    started_at: datetime | None = None
    finished_at: datetime | None = None
    event_at: datetime = Field(default_factory=utcnow)
    event_seq: int = 0


class JobCreate(BaseDTO):
    kind: str
    status: JobStatus = JobStatus.PENDING
    progress: float | None = None
    message: str | None = None
    subject: str | None = None
    durable_run_id: str | None = None
    error: str | None = None
    heartbeat_at: datetime
    started_at: datetime | None = None
    finished_at: datetime | None = None
    event_at: datetime
    event_seq: int = 0


class JobUpdate(BaseDTO):
    status: JobStatus | None = None
    progress: float | None = None
    message: str | None = None
    subject: str | None = None
    durable_run_id: str | None = None
    error: str | None = None
    heartbeat_at: datetime | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    event_at: datetime | None = None
    event_seq: int | None = None


class JobRecord(ReadDocument):
    """One long-running operation, as an observer sees it.

    The document's ``id`` **is** the job id: a job is looked up, created and merged by
    primary key, so a projector never has to search for the row it is about to write.
    """

    kind: str
    """App-defined vocabulary for what kind of work this is (``"export"``, ``"reencrypt"``)."""

    status: JobStatus = JobStatus.PENDING
    """Where the job is (see :class:`JobStatus`)."""

    progress: float | None = None
    """Fraction complete in ``0.0..1.0``; ``None`` is indeterminate (a spinner, not a bar)."""

    message: str | None = None
    """The last human-readable line. Overwritable — messages are not monotonic."""

    subject: str | None = None
    """Free-form reference to what the job is *about* (an archive path, a tenant, a route)."""

    durable_run_id: str | None = None
    """The durable run currently carrying this job, when it runs durably.

    A link, never an identity: across a terminate-and-resume pause this names a different
    run each round while the job — and its ``progress`` — stays the same.
    """

    error: str | None = None
    """Why it failed, when :attr:`status` is :attr:`JobStatus.FAILED`."""

    heartbeat_at: datetime
    """The newest report seen from this job — the "is it stuck?" query.

    Monotonic: a straggling out-of-order tick can never pull it backwards, because a job
    that is still reporting must not read as stale just because the transport reordered
    two of its ticks.
    """

    started_at: datetime | None = None
    """When the job first left :attr:`JobStatus.PENDING` (the earliest such report)."""

    finished_at: datetime | None = None
    """When it reached a terminal status."""

    event_at: datetime
    """Source timestamp of the event that last won the merge — the record's merge position."""

    event_seq: int = 0
    """Per-reporter sequence of that event; breaks ties when the clock cannot.

    Under a frozen or coarse clock every event in a burst carries the same ``event_at``,
    and without this the projector could not order ``waiting`` after ``running`` within
    one instant — the reversible transition would be silently dropped.
    """


# ....................... #

JobDocumentSpec = DocumentSpec[JobRecord, JobDoc, JobCreate, JobUpdate]
"""The job collection's spec type (only the read model is public)."""

_INDEXED_FIELDS: Final = frozenset(
    {"kind", "status", "progress", "heartbeat_at", "event_at", "event_seq"}
)
"""Fields the projector and the staleness query filter, sort or compare on."""


def job_record_spec(
    name: str = DEFAULT_JOB_COLLECTION,
    *,
    encryption: FieldEncryption | None = None,
) -> JobDocumentSpec:
    """The document collection holding job records (wire it tenant-aware).

    A job's ``message`` and ``subject`` are the fields that carry business meaning — "12 of
    40 invoices for Acme", an archive path — so *encryption* is the seam for an application
    that seals its other collections. The status/progress/heartbeat fields are the record's
    query surface: the projector compares them to merge, and the staleness sweep filters and
    sorts on them, so sealing one turns every merge and every "which jobs are stuck?" query
    into a comparison over ciphertext. That is refused here rather than discovered as a
    wrong answer at 3am.
    """

    if encryption is not None and (forbidden := encryption.sealed & _INDEXED_FIELDS):
        raise exc.configuration(
            f"job_record_spec cannot seal {sorted(forbidden)}: the projector merges on "
            "status/progress/event_at/event_seq and the staleness query filters on "
            "kind/status/heartbeat_at — sealed, both would compare ciphertext and answer "
            "wrongly rather than fail. Seal 'message' / 'subject' / 'error' instead.",
            code="job_record_sealed_index",
        )

    return DocumentSpec(
        name=name,
        read=JobRecord,
        write=DocumentWriteTypes(
            domain=JobDoc,
            create_cmd=JobCreate,
            update_cmd=JobUpdate,
        ),
        encryption=encryption,
    )
