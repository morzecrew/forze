"""Operation progress — a job read model, one declared event, a reporter, a projector.

Long-running work is opaque by default: a durable run exposes pending/running/terminal and
nothing between, so an export walking millions of rows, a re-encryption sweep or an index
rebuild is a log line until it ends. The pieces here give that work an observable shape
without giving it a controller:

- :func:`job_record_spec` — the document collection, one :class:`JobRecord` per job.
- :data:`JOB_PROGRESS_EVENT` — the one declared realtime event (``job.progress``).
- :class:`ProgressReporter` — the write side, with coalescing and a monotonic clamp.
- :class:`JobProgressProjector` — the read side, owning the out-of-order merge rules.

A job record only ever *watches*: nothing here cancels, schedules, retries or aggregates,
and a job's status vocabulary is deliberately its own rather than a durable run's. A job is
**task-grained** — it is linked to a durable run by id and outlives it, which is what lets
one record span a terminate-and-resume pause (``waiting``) instead of showing a task that
is waiting on a human as finished.
"""

from .events import JOB_PROGRESS_EVENT, JOB_PROGRESS_EVENT_NAME, JobProgress
from .projector import JobProgressProjector, build_job_progress_projector
from .record import (
    DEFAULT_JOB_COLLECTION,
    JobDocumentSpec,
    JobRecord,
    JobStatus,
    job_record_spec,
)
from .reporter import (
    DEFAULT_MIN_INTERVAL,
    DEFAULT_PROGRESS_ROUTE,
    JobProgressMerger,
    ProgressReporter,
    ProgressSink,
    RealtimeProgressSink,
    RecordProgressSink,
    build_progress_reporter,
    job_topic,
    progress_outbox_spec,
)

# ----------------------- #

__all__ = [
    "DEFAULT_JOB_COLLECTION",
    "DEFAULT_MIN_INTERVAL",
    "DEFAULT_PROGRESS_ROUTE",
    "JOB_PROGRESS_EVENT",
    "JOB_PROGRESS_EVENT_NAME",
    "JobDocumentSpec",
    "JobProgress",
    "JobProgressMerger",
    "JobProgressProjector",
    "JobRecord",
    "JobStatus",
    "ProgressReporter",
    "ProgressSink",
    "RealtimeProgressSink",
    "RecordProgressSink",
    "build_job_progress_projector",
    "build_progress_reporter",
    "job_record_spec",
    "job_topic",
    "progress_outbox_spec",
]
