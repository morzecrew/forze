"""The staleness signal: the sweep, what it counts, and what a dead sweep looks like.

# covers: forze_kits.integrations.progress.observability.JobStalenessMonitor
# covers: forze_kits.integrations.progress.observability.instrument_job_progress
# covers: forze_kits.integrations.progress.lifecycle.job_staleness_lifecycle_step

Two things here are worth more than "does the gauge move". The first is that the count is
the **whole** count: a metric built from a capped page saturates at the page size, so it is
calmest exactly when the most work is stuck. The second is that a monitor whose sweep loop
has died reports its last answer forever — usually zero, which reads as good news — so the
freshness of the answer is exported alongside it and asserted here.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any, Final
from uuid import UUID, uuid4

import pytest

from forze.application.execution import DepsRegistry, ExecutionContext, ExecutionRuntime
from forze.base.exceptions import CoreException
from forze.base.primitives import bind_time_source, uuid7
from forze_kits.integrations.progress import (
    JOBS_OLDEST_SILENCE_GAUGE,
    JOBS_SCAN_AGE_GAUGE,
    JOBS_STALLED_GAUGE,
    JobProgress,
    JobStalenessMonitor,
    JobStalenessStats,
    JobStatus,
    build_job_progress_projector,
    instrument_job_progress,
    job_record_spec,
    job_staleness_lifecycle_step,
)
from forze_mock import MockDepsModule

# ----------------------- #

_SPEC: Final = job_record_spec()
_T0: Final = datetime(2026, 8, 4, 12, 0, 0, tzinfo=UTC)
_WINDOW: Final = timedelta(minutes=15)


def _runtime() -> ExecutionRuntime:
    return ExecutionRuntime(deps=DepsRegistry.from_modules(MockDepsModule()).freeze())


class _Clock:
    """A wall clock the test drives, so "how long has this job been silent" is exact."""

    def __init__(self, instant: datetime = _T0) -> None:
        self.instant = instant

    def now(self) -> datetime:
        return self.instant

    def uuid(self) -> Any:
        return uuid7(timestamp_ns=int(self.instant.timestamp() * 1_000_000_000))

    def monotonic(self) -> float:
        return 1_000.0

    def advance(self, **kwargs: float) -> None:
        self.instant += timedelta(**kwargs)


class _CapturingMeter:
    """Stands in for the OTel meter: keeps each gauge's callback so a test can scrape it."""

    def __init__(self) -> None:
        self.gauges: dict[str, Any] = {}

    def create_observable_gauge(
        self, name: str, *, callbacks: list[Any], unit: str, description: str
    ) -> None:
        self.gauges[name] = callbacks[0]

    def scrape(self, name: str) -> list[tuple[float, dict[str, Any]]]:
        return [
            (observation.value, dict(observation.attributes or {}))
            for observation in self.gauges[name](None)
        ]


async def _report(
    ctx: ExecutionContext,
    *,
    kind: str,
    status: JobStatus,
    at: datetime,
    job_id: UUID | None = None,
) -> UUID:
    """Land one job's report in the collection as of *at*, without a reporter in the way.

    Returns the job id so a test can report *again* for the same job — the difference
    between a job that came back to life and a second job that never did.
    """

    resolved = job_id if job_id is not None else uuid4()

    await build_job_progress_projector(ctx, spec=_SPEC).apply(
        JobProgress(job_id=resolved, kind=kind, status=status, at=at, seq=1)
    )

    return resolved


# ----------------------- #


class TestWhatTheSweepCounts:
    async def test_the_count_is_the_whole_count_not_a_page(self) -> None:
        # The trap this exists to close: `len(find_stalled(...))` saturates at the page
        # size, so a fleet with 500 stuck jobs and a fleet with 100 look identical — and the
        # gauge stops rising at exactly the point the operator most needs it to.
        clock = _Clock()
        runtime = _runtime()
        monitor = JobStalenessMonitor(silent_after=_WINDOW, spec=_SPEC)

        with bind_time_source(clock):
            async with runtime.scope():
                ctx = runtime.get_context()

                for _ in range(120):
                    await _report(ctx, kind="export", status=JobStatus.RUNNING, at=_T0)

                clock.advance(hours=1)
                await monitor.sweep(ctx)

        assert monitor.stats().stalled == 120  # not 100, the default page size

    async def test_only_started_unfinished_and_silent_jobs_count(self) -> None:
        clock = _Clock()
        runtime = _runtime()
        monitor = JobStalenessMonitor(silent_after=_WINDOW, spec=_SPEC)

        with bind_time_source(clock):
            async with runtime.scope():
                ctx = runtime.get_context()

                await _report(ctx, kind="export", status=JobStatus.RUNNING, at=_T0)
                await _report(ctx, kind="export", status=JobStatus.WAITING, at=_T0)
                await _report(ctx, kind="export", status=JobStatus.SUCCEEDED, at=_T0)
                await _report(ctx, kind="export", status=JobStatus.FAILED, at=_T0)

                clock.advance(hours=1)
                await _report(ctx, kind="export", status=JobStatus.RUNNING, at=clock.instant)

                await monitor.sweep(ctx)

        # A finished job is never stuck however long ago it finished; a job still reporting
        # is not stuck either. A `waiting` one *is* — a task paused on an answer nobody gives
        # is exactly what an operator wants to hear about.
        assert monitor.stats().stalled == 2

    async def test_the_oldest_silence_says_how_bad_not_just_how_many(self) -> None:
        clock = _Clock()
        runtime = _runtime()
        monitor = JobStalenessMonitor(silent_after=_WINDOW, spec=_SPEC)

        with bind_time_source(clock):
            async with runtime.scope():
                ctx = runtime.get_context()

                await _report(ctx, kind="export", status=JobStatus.RUNNING, at=_T0)
                clock.advance(hours=3)
                await _report(ctx, kind="export", status=JobStatus.RUNNING, at=clock.instant)

                clock.advance(minutes=30)
                await monitor.sweep(ctx)

        stats = monitor.stats()

        # Two stuck jobs, and one of them has been silent for three and a half hours — the
        # count alone cannot tell that from two jobs a minute past the threshold.
        assert stats.stalled == 2
        assert stats.oldest_silence == pytest.approx(timedelta(hours=3.5).total_seconds())

    async def test_a_quiet_collection_reports_zero_not_stale_numbers(self) -> None:
        clock = _Clock()
        runtime = _runtime()
        monitor = JobStalenessMonitor(silent_after=_WINDOW, spec=_SPEC)

        with bind_time_source(clock):
            async with runtime.scope():
                ctx = runtime.get_context()

                job = await _report(ctx, kind="export", status=JobStatus.RUNNING, at=_T0)
                clock.advance(hours=1)
                await monitor.sweep(ctx)

                assert monitor.stats().stalled == 1

                # The same job reports again — the sweep must forget, not accumulate.
                await _report(
                    ctx, kind="export", status=JobStatus.RUNNING, at=clock.instant, job_id=job
                )
                await monitor.sweep(ctx)

        assert monitor.stats() == JobStalenessStats()  # back to the zero snapshot

    async def test_declared_kinds_are_counted_separately(self) -> None:
        clock = _Clock()
        runtime = _runtime()
        monitor = JobStalenessMonitor(
            silent_after=_WINDOW, spec=_SPEC, kinds=("export", "reencrypt")
        )

        with bind_time_source(clock):
            async with runtime.scope():
                ctx = runtime.get_context()

                await _report(ctx, kind="export", status=JobStatus.RUNNING, at=_T0)
                await _report(ctx, kind="reencrypt", status=JobStatus.RUNNING, at=_T0)
                await _report(ctx, kind="reencrypt", status=JobStatus.RUNNING, at=_T0)
                await _report(ctx, kind="snapshot", status=JobStatus.RUNNING, at=_T0)

                clock.advance(hours=1)
                await monitor.sweep(ctx)

        assert monitor.stats("export").stalled == 1
        assert monitor.stats("reencrypt").stalled == 2
        # An undeclared kind is not counted anywhere — cardinality is a wiring decision, so
        # a kind nobody named is invisible rather than silently added to a label set.
        assert monitor.stats("snapshot").stalled == 0

    def test_a_window_of_zero_is_refused(self) -> None:
        with pytest.raises(CoreException) as err:
            JobStalenessMonitor(silent_after=timedelta(0), spec=_SPEC)

        assert err.value.kind.value == "configuration"


# ....................... #


class TestTheGauges:
    async def test_the_gauges_report_what_the_last_sweep_found(self) -> None:
        clock = _Clock()
        runtime = _runtime()
        monitor = JobStalenessMonitor(silent_after=_WINDOW, spec=_SPEC, kinds=("export",))
        meter = _CapturingMeter()
        instrument_job_progress(monitor, meter=meter)  # type: ignore[arg-type]

        with bind_time_source(clock):
            async with runtime.scope():
                ctx = runtime.get_context()

                # Before the first sweep: nothing known, and the scan age says so rather
                # than claiming a fresh zero.
                assert meter.scrape(JOBS_STALLED_GAUGE) == [(0.0, {"forze.job.kind": "export"})]
                assert meter.scrape(JOBS_SCAN_AGE_GAUGE) == [(-1.0, {})]

                await _report(ctx, kind="export", status=JobStatus.RUNNING, at=_T0)
                clock.advance(hours=1)
                await monitor.sweep(ctx)

                assert meter.scrape(JOBS_STALLED_GAUGE) == [(1.0, {"forze.job.kind": "export"})]
                assert meter.scrape(JOBS_OLDEST_SILENCE_GAUGE) == [
                    (timedelta(hours=1).total_seconds(), {"forze.job.kind": "export"})
                ]
                assert meter.scrape(JOBS_SCAN_AGE_GAUGE) == [(0.0, {})]

    async def test_a_dead_sweep_is_visible_in_the_scan_age(self) -> None:
        # The failure this signal exists for: the loop dies, the stalled gauge freezes at
        # its last value — almost always zero — and every dashboard built on it goes green
        # at the moment it stopped knowing anything. Only the scan age says otherwise.
        clock = _Clock()
        runtime = _runtime()
        monitor = JobStalenessMonitor(silent_after=_WINDOW, spec=_SPEC)
        meter = _CapturingMeter()
        instrument_job_progress(monitor, meter=meter)  # type: ignore[arg-type]

        with bind_time_source(clock):
            async with runtime.scope():
                await monitor.sweep(runtime.get_context())

            clock.advance(hours=6)  # nothing sweeps again

            assert meter.scrape(JOBS_STALLED_GAUGE) == [(0.0, {})]  # reassuring, and stale
            assert meter.scrape(JOBS_SCAN_AGE_GAUGE) == [
                (timedelta(hours=6).total_seconds(), {})
            ]

    async def test_the_scan_age_is_reported_once_not_once_per_kind(self) -> None:
        # One sweep answers for every kind, so labelling this by kind would report the same
        # number N times and fire one alert per kind for a single dead loop.
        monitor = JobStalenessMonitor(silent_after=_WINDOW, spec=_SPEC, kinds=("a", "b", "c"))
        meter = _CapturingMeter()
        instrument_job_progress(monitor, meter=meter)  # type: ignore[arg-type]

        assert len(meter.scrape(JOBS_STALLED_GAUGE)) == 3
        assert len(meter.scrape(JOBS_SCAN_AGE_GAUGE)) == 1


# ....................... #


class TestTheLifecycleStep:
    async def test_the_step_sweeps_and_stops(self) -> None:
        runtime = _runtime()
        step, monitor = job_staleness_lifecycle_step(
            silent_after=timedelta(milliseconds=1),
            spec=_SPEC,
            interval=timedelta(milliseconds=5),
            jitter=0.0,
        )

        async with runtime.scope():
            ctx = runtime.get_context()
            await _report(ctx, kind="export", status=JobStatus.RUNNING, at=_T0)

            await step.startup(ctx)

            for _ in range(200):
                await asyncio.sleep(0.005)

                if monitor.stats().stalled:
                    break

            await step.shutdown(ctx)

        assert monitor.stats().stalled == 1
        assert monitor.scan_age() >= 0.0  # it swept at least once
        assert step.requires_long_running

    async def test_the_step_and_the_monitor_come_as_a_pair(self) -> None:
        # Handing back both is the point: a sweep nobody instruments and a gauge nobody
        # refreshes are the two ways this signal silently does nothing.
        step, monitor = job_staleness_lifecycle_step(silent_after=_WINDOW)

        assert step.id == "job_staleness"
        assert monitor.silent_after == _WINDOW

    def test_a_nonsense_schedule_is_refused(self) -> None:
        with pytest.raises(CoreException):
            job_staleness_lifecycle_step(silent_after=_WINDOW, interval=timedelta(0))

        with pytest.raises(CoreException):
            job_staleness_lifecycle_step(silent_after=_WINDOW, jitter=1.0)
