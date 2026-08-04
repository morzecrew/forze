"""Turn the staleness query into a signal something can alarm on.

`find_stalled` answers "is anything stuck?" only when somebody asks. A monitor asks on a
schedule and keeps the answer; :func:`instrument_job_progress` exports it as OpenTelemetry
gauges. Two shapes are forced on this by what the pieces are:

- **The counts read a cache, not the store.** An OTel observable callback is synchronous and
  the staleness query is not, so the count a scrape sees is the last sweep's, and the
  *sweep* interval — not the scrape interval — is its resolution. The one number that must
  not be cached is the *age* of the worst offender: stored as an age it would hold a flat
  line while a job got steadily worse, so the sweep stores the instant and the gauge
  subtracts at scrape.
- **A cache that stops being refreshed reads exactly like good news.** A dead sweep loop
  leaves the stalled gauge frozen at whatever it last saw, most likely zero, which is the
  quietest possible failure. So the freshness of the answer is itself exported
  (``forze.jobs.staleness.scan_age``, computed at scrape time from a stored timestamp) and
  the alert wants both: stuck jobs *or* an answer that has stopped being updated.

Per the observability doctrine, no metric here is labelled by tenant. A per-tenant sweep
aggregates into one number instead (see :class:`JobStalenessMonitor`), and the only optional
label is the job ``kind`` — declared up front, so its cardinality is a wiring decision rather
than a function of what an application happens to run.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Sequence
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, final
from uuid import UUID

import attrs

from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import ExecutionContext
from forze.base.exceptions import exc
from forze.base.primitives import utcnow

from .projector import build_job_progress_projector
from .record import JobDocumentSpec, job_record_spec

if TYPE_CHECKING:
    from opentelemetry.metrics import CallbackOptions, Meter, Observation

# ----------------------- #

JOBS_STALLED_GAUGE = "forze.jobs.stalled"
JOBS_OLDEST_SILENCE_GAUGE = "forze.jobs.stalled.oldest_silence"
JOBS_SCAN_AGE_GAUGE = "forze.jobs.staleness.scan_age"

_KIND_ATTRIBUTE = "forze.job.kind"

# ....................... #


@final
@attrs.define(slots=True, frozen=True)
class JobStalenessStats:
    """One sweep's answer for one job kind — an immutable snapshot, read at scrape time."""

    stalled: int = 0
    """Jobs that started, have not finished, and have not reported inside the window.

    The **whole** count, not a page of one: a gauge built from a capped page saturates at
    the page size and looks calmest exactly when things are worst.
    """

    oldest_heartbeat_at: datetime | None = None
    """When the quietest stuck job last reported; ``None`` when nothing is stuck.

    Stored as the **instant**, not as an age, because an age computed at sweep time and read
    at scrape time is a number that stops moving: a job going from one hour silent to five
    would hold a flat line between sweeps, and the only way to read it correctly would be to
    add :meth:`JobStalenessMonitor.scan_age` to it, which no dashboard does. Kept as a
    timestamp, :meth:`silence` gives the true age at whatever moment asks.
    """

    # ....................... #

    def silence(self, *, now: datetime | None = None) -> float:
        """Seconds since the quietest stuck job reported, as of *now*; ``0`` when none is.

        The count says how many, this says how bad — and it is the one that distinguishes a
        handful of jobs a minute past the threshold from one that died overnight.
        """

        if self.oldest_heartbeat_at is None:
            return 0.0

        return max(
            0.0, ((now if now is not None else utcnow()) - self.oldest_heartbeat_at).total_seconds()
        )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True)  # not frozen — holds the sweep's answers
class JobStalenessMonitor:
    """Sweep the job collection for stuck work and hold the answer for the gauges.

    Built by :func:`~.lifecycle.job_staleness_lifecycle_step`, which drives :meth:`sweep` on
    an interval; :func:`instrument_job_progress` exports what it holds. Usable on its own
    where an application drives its own schedule.
    """

    silent_after: timedelta
    """How long a started, unfinished job may go without reporting before it counts as stuck.

    There is no safe default: the right window is a multiple of how often the work in
    question reports, and a window shorter than the reporter's own coalescing interval would
    call every healthy job stuck.
    """

    spec: JobDocumentSpec = attrs.field(factory=job_record_spec)
    """The job collection to sweep."""

    kinds: tuple[str, ...] = ()
    """Job kinds to report separately. Empty (the default) sweeps every kind as one number.

    Declared rather than discovered: ``kind`` is application vocabulary, so labelling by
    whatever the collection happens to contain makes metric cardinality a function of
    runtime data. Naming the kinds you watch bounds it at wiring time.
    """

    tenants: Callable[[], Sequence[UUID]] | None = None
    """Tenants to sweep bound, for a ``tenant_aware`` collection (the retention-sweep shape).

    Their results are **summed** into one set of numbers rather than labelled per tenant —
    the tenant is never a metric label. The gauges then say "N jobs are stuck across this
    shard", and *which* ones is the record's own query to answer.
    """

    _stats: dict[str | None, JobStalenessStats] = attrs.field(factory=dict, init=False)
    _swept_at: datetime | None = attrs.field(default=None, init=False)

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.silent_after.total_seconds() <= 0:
            raise exc.configuration("JobStalenessMonitor silent_after must be positive")

    # ....................... #

    @property
    def keys(self) -> tuple[str | None, ...]:
        """The label values this monitor reports — the declared kinds, or one unlabelled set."""

        return self.kinds if self.kinds else (None,)

    # ....................... #

    def stats(self, kind: str | None = None) -> JobStalenessStats:
        """The last sweep's answer for *kind* (zeroes before the first sweep)."""

        return self._stats.get(kind, JobStalenessStats())

    # ....................... #

    def scan_age(self) -> float:
        """Seconds since the last successful sweep; ``-1`` before the first one.

        The freshness of every other number here. A negative value is deliberately not zero:
        "never swept" and "swept just now" are opposite states and a dashboard that shows
        them identically is how a monitor that never started passes for a healthy one.
        """

        if self._swept_at is None:
            return -1.0

        return max(0.0, (utcnow() - self._swept_at).total_seconds())

    # ....................... #

    async def sweep(self, ctx: ExecutionContext) -> None:
        """Refresh the answers. Two indexed reads per kind, whatever the collection's size."""

        answers = {key: JobStalenessStats() for key in self.keys}

        if self.tenants is None:
            for key in self.keys:
                answers[key] = await self._measure(ctx, key)

        else:
            for tenant in self.tenants():
                with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=tenant)):
                    for key in self.keys:
                        answers[key] = _merged(answers[key], await self._measure(ctx, key))

        self._stats = answers
        self._swept_at = utcnow()

    # ....................... #

    async def _measure(self, ctx: ExecutionContext, kind: str | None) -> JobStalenessStats:
        projector = build_job_progress_projector(ctx, spec=self.spec)
        cutoff = utcnow() - self.silent_after

        stalled = await projector.count_stalled(silent_since=cutoff, kind=kind)

        if not stalled:
            return JobStalenessStats()

        # The quietest row only — the count already came from the index, and this is asking
        # a different question ("how bad") that one row answers.
        quietest = await projector.find_stalled(silent_since=cutoff, kind=kind, limit=1)

        return JobStalenessStats(
            stalled=stalled,
            oldest_heartbeat_at=quietest[0].heartbeat_at if quietest else None,
        )


def _merged(left: JobStalenessStats, right: JobStalenessStats) -> JobStalenessStats:
    """Fold one tenant's answer into the shard's: counts add, the quietest job wins.

    "Worst" is the **earliest** heartbeat across the shard — the min, since these are
    instants now rather than ages.
    """

    heartbeats = [
        at for at in (left.oldest_heartbeat_at, right.oldest_heartbeat_at) if at is not None
    ]

    return JobStalenessStats(
        stalled=left.stalled + right.stalled,
        oldest_heartbeat_at=min(heartbeats) if heartbeats else None,
    )


# ....................... #


def instrument_job_progress(
    monitor: JobStalenessMonitor,
    *,
    meter: Meter | None = None,
) -> None:
    """Export *monitor*'s answers as OTel observable gauges. Call once at assembly.

    Emits via the global OTel meter unless *meter* is supplied. OpenTelemetry is imported
    lazily, so this module costs an uninstrumented application nothing at import.

    - ``forze.jobs.stalled`` — started, unfinished, and silent past the window.
    - ``forze.jobs.stalled.oldest_silence`` — seconds since the quietest one reported,
      computed **at scrape time** from the stored heartbeat, so it keeps climbing between
      sweeps instead of holding the age it had when the sweep ran.
    - ``forze.jobs.staleness.scan_age`` — seconds since the last sweep (``-1`` = never).

    **Alarm on the third one too.** The counts are a cache; if the sweep loop dies they
    freeze at their last value — almost always zero — and a dashboard built on them alone
    goes green at the moment it stops knowing anything.
    """

    from opentelemetry import metrics

    resolved = meter or metrics.get_meter("forze")

    def _gauge(
        name: str,
        pick: Callable[[str | None], float],
        description: str,
        *,
        unit: str,
    ) -> None:
        def callback(_options: CallbackOptions) -> Iterable[Observation]:
            return [
                metrics.Observation(pick(key), {_KIND_ATTRIBUTE: key} if key is not None else {})
                for key in monitor.keys
            ]

        resolved.create_observable_gauge(
            name, callbacks=[callback], unit=unit, description=description
        )

    _gauge(
        JOBS_STALLED_GAUGE,
        lambda kind: float(monitor.stats(kind).stalled),
        "Jobs that started, have not finished, and have not reported inside the window.",
        unit="1",
    )
    _gauge(
        JOBS_OLDEST_SILENCE_GAUGE,
        # Computed here, at scrape: the sweep stores *when* the quietest job reported, so a
        # job going from one hour silent to five shows it without waiting for a sweep.
        lambda kind: monitor.stats(kind).silence(),
        "Seconds since the quietest stuck job last reported (0 when nothing is stuck).",
        unit="s",
    )

    # Not per kind: one sweep answers for all of them, so labelling this by kind would
    # report the same number N times and invite an alert that fires N times for one loop.
    def _scan_age(_options: CallbackOptions) -> Iterable[Observation]:
        return [metrics.Observation(monitor.scan_age(), {})]

    resolved.create_observable_gauge(
        JOBS_SCAN_AGE_GAUGE,
        callbacks=[_scan_age],
        unit="s",
        description="Seconds since the staleness sweep last succeeded (-1 before the first).",
    )
