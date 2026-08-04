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
import sys
from datetime import UTC, datetime, timedelta
from typing import Any, Final
from uuid import UUID, uuid4

import pytest

from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import DepsRegistry, ExecutionContext, ExecutionRuntime
from forze.base.exceptions import CoreException, exc
from forze.base.primitives import bind_time_source, uuid7
from forze_kits.integrations.progress import (
    JOBS_OLDEST_SILENCE_GAUGE,
    JOBS_SCAN_AGE_GAUGE,
    JOBS_STALLED_GAUGE,
    JobProgress,
    JobStalenessMonitor,
    JobStalenessStats,
    JobStatus,
    OTHER_KIND_LABEL,
    build_job_progress_projector,
    instrument_job_progress,
    job_record_spec,
    job_staleness_lifecycle_step,
)
from forze_kits.integrations.progress import lifecycle as lifecycle_module
from forze_kits.integrations.progress import observability
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


async def _settle(task: Any, *, ticks: int = 200) -> None:
    """Wait for a loop task to finish on its own, without pinning a wall-clock duration."""

    for _ in range(ticks):
        if task is not None and task.done():
            return

        await asyncio.sleep(0.002)


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

            # Two stuck jobs, and one of them has been silent for three and a half hours —
            # the count alone cannot tell that from two jobs a minute past the threshold.
            assert stats.stalled == 2
            assert stats.silence() == pytest.approx(timedelta(hours=3.5).total_seconds())

    async def test_the_silence_keeps_climbing_between_sweeps(self) -> None:
        # An age computed at sweep time and read at scrape time stops moving: a job going
        # from one hour silent to five would hold a flat line until the next sweep, and the
        # only way to read it correctly would be to add the scan age to it, which no
        # dashboard does. The sweep stores the *instant*; the age is computed when asked.
        clock = _Clock()
        runtime = _runtime()
        monitor = JobStalenessMonitor(silent_after=_WINDOW, spec=_SPEC)

        with bind_time_source(clock):
            async with runtime.scope():
                ctx = runtime.get_context()

                await _report(ctx, kind="export", status=JobStatus.RUNNING, at=_T0)
                clock.advance(hours=1)
                await monitor.sweep(ctx)

                assert monitor.stats().silence() == pytest.approx(3600.0)

                clock.advance(hours=4)  # no sweep in between

                assert monitor.stats().silence() == pytest.approx(5 * 3600.0)
                assert monitor.stats().stalled == 1  # the count is still the sweep's

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

        # An undeclared kind gets no label of its own — cardinality stays a wiring decision —
        # but it is still *watched*: it lands in the catch-all rather than nowhere. Declaring
        # kinds otherwise means a kind added later, or misspelled at the call site, is stuck
        # and reads as a healthy zero.
        assert monitor.stats("snapshot").stalled == 0
        assert monitor.stats(OTHER_KIND_LABEL).stalled == 1

        # And the buckets partition the collection, so the label can still be summed.
        assert sum(monitor.stats(key).stalled for key in monitor.keys) == 4

    async def test_a_sweep_never_publishes_half_of_each_snapshot(self, mocker: Any) -> None:
        # Two reads against a live collection: the count comes back, the jobs it counted
        # report in, and the page of the quietest one comes back empty. Published as they
        # arrived, that pair says "five jobs are stuck, the worst of them for zero seconds"
        # — a contradiction between two panels that costs an operator a morning.
        class _VanishingProjector:
            async def count_stalled(self, **_kwargs: Any) -> int:
                return 5

            async def find_stalled(self, **_kwargs: Any) -> list[Any]:
                return []

        runtime = _runtime()
        monitor = JobStalenessMonitor(silent_after=_WINDOW, spec=_SPEC)
        mocker.patch.object(
            observability, "build_job_progress_projector", return_value=_VanishingProjector()
        )

        async with runtime.scope():
            await monitor.sweep(runtime.get_context())

        # The empty page is the later, truer answer — so both numbers say so.
        assert monitor.stats() == JobStalenessStats()

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
                assert meter.scrape(JOBS_STALLED_GAUGE) == [
                    (0.0, {"forze.job.kind": "export"}),
                    (0.0, {"forze.job.kind": OTHER_KIND_LABEL}),
                ]
                assert meter.scrape(JOBS_SCAN_AGE_GAUGE) == [(-1.0, {})]

                await _report(ctx, kind="export", status=JobStatus.RUNNING, at=_T0)
                clock.advance(hours=1)
                await monitor.sweep(ctx)

                assert meter.scrape(JOBS_STALLED_GAUGE) == [
                    (1.0, {"forze.job.kind": "export"}),
                    (0.0, {"forze.job.kind": OTHER_KIND_LABEL}),
                ]
                assert meter.scrape(JOBS_OLDEST_SILENCE_GAUGE) == [
                    (timedelta(hours=1).total_seconds(), {"forze.job.kind": "export"}),
                    (0.0, {"forze.job.kind": OTHER_KIND_LABEL}),
                ]
                assert meter.scrape(JOBS_SCAN_AGE_GAUGE) == [(0.0, {})]

                # The scrape, not the sweep, is when the age is computed.
                clock.advance(hours=2)

                assert meter.scrape(JOBS_OLDEST_SILENCE_GAUGE) == [
                    (timedelta(hours=3).total_seconds(), {"forze.job.kind": "export"}),
                    (0.0, {"forze.job.kind": OTHER_KIND_LABEL}),
                ]

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
            assert meter.scrape(JOBS_SCAN_AGE_GAUGE) == [(timedelta(hours=6).total_seconds(), {})]

    async def test_the_scan_age_is_reported_once_not_once_per_kind(self) -> None:
        # One sweep answers for every kind, so labelling this by kind would report the same
        # number N times and fire one alert per kind for a single dead loop.
        monitor = JobStalenessMonitor(silent_after=_WINDOW, spec=_SPEC, kinds=("a", "b", "c"))
        meter = _CapturingMeter()
        instrument_job_progress(monitor, meter=meter)  # type: ignore[arg-type]

        assert len(meter.scrape(JOBS_STALLED_GAUGE)) == 4  # three kinds, plus the catch-all
        assert len(meter.scrape(JOBS_SCAN_AGE_GAUGE)) == 1


# ....................... #


class TestAPerTenantSweep:
    """A tenant-partitioned collection is swept bound, once per tenant, and folded into one."""

    async def test_the_shard_folds_its_tenants_instead_of_replacing_them(self) -> None:
        # The tenant is never a metric label, so a per-tenant sweep has to *add up* rather
        # than publish a series each: the gauge says "N jobs are stuck on this shard" and
        # the record answers which. The bug this shape invites is a loop that assigns where
        # it should fold — every tenant overwriting the last, so a shard reports whichever
        # tenant it happened to sweep last and every other one is invisible.
        #
        # Asserted as "more than one tenant's worth, and the worst instant anywhere", not as
        # a literal count: this collection is not tenant-partitioned, so both bindings read
        # the same rows. The property holds either way; a count would only pin the mock.
        first, second = uuid4(), uuid4()
        clock = _Clock()
        runtime = _runtime()
        solo = JobStalenessMonitor(silent_after=_WINDOW, spec=_SPEC, tenants=lambda: [first])
        shard = JobStalenessMonitor(
            silent_after=_WINDOW, spec=_SPEC, tenants=lambda: [first, second]
        )

        with bind_time_source(clock):
            async with runtime.scope():
                ctx = runtime.get_context()

                with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=first)):
                    await _report(ctx, kind="export", status=JobStatus.RUNNING, at=_T0)
                    await _report(ctx, kind="export", status=JobStatus.RUNNING, at=_T0)

                clock.advance(hours=2)

                with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=second)):
                    await _report(ctx, kind="export", status=JobStatus.RUNNING, at=clock.instant)

                clock.advance(hours=1)
                await solo.sweep(ctx)
                await shard.sweep(ctx)

            assert shard.stats().stalled > solo.stats().stalled
            # And the worst offender is the earliest heartbeat anywhere on the shard — a
            # `max` here, or a last-writer-wins, would report the shard as three hours
            # healthier than it is.
            assert shard.stats().oldest_heartbeat_at == _T0
            assert shard.stats().silence() == pytest.approx(timedelta(hours=3).total_seconds())

    async def test_a_tenant_arriving_re_arms_the_empty_shard_warning(self, mocker: Any) -> None:
        # Once per streak, not once per process: a shard that is idle, then busy, then idle
        # again has something new to say the second time.
        tenants: list[UUID] = []
        runtime = _runtime()
        monitor = JobStalenessMonitor(silent_after=_WINDOW, spec=_SPEC, tenants=lambda: tenants)
        logger = mocker.patch.object(observability, "logger")

        async with runtime.scope():
            ctx = runtime.get_context()

            await monitor.sweep(ctx)  # empty: says so
            tenants.append(uuid4())
            await monitor.sweep(ctx)  # busy: the streak is over
            tenants.clear()
            await monitor.sweep(ctx)  # empty again: worth saying again

        empty_warnings = [
            call
            for call in logger.warning.call_args_list
            if "no tenants to examine" in call.args[0]
        ]

        assert len(empty_warnings) == 2


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

    def test_instrumenting_the_same_monitor_twice_is_refused(self) -> None:
        # Instruments register per name, so a second set of gauges over one sweep reports
        # every kind twice and doubles anything built on a rate.
        monitor = JobStalenessMonitor(silent_after=_WINDOW, spec=_SPEC)
        instrument_job_progress(monitor, meter=_CapturingMeter())  # type: ignore[arg-type]

        with pytest.raises(CoreException) as err:
            instrument_job_progress(monitor, meter=_CapturingMeter())  # type: ignore[arg-type]

        assert err.value.code == "progress_monitor_instrumented_twice"

    async def test_a_shard_with_no_tenants_says_so_once(self, mocker: Any) -> None:
        # "Examined nothing" and "examined everything and found nothing" both publish a
        # zero. A tenant provider returning an empty list by mistake would otherwise be
        # indistinguishable from a healthy, idle fleet.
        runtime = _runtime()
        monitor = JobStalenessMonitor(silent_after=_WINDOW, spec=_SPEC, tenants=lambda: [])
        logger = mocker.patch.object(observability, "logger")

        async with runtime.scope():
            ctx = runtime.get_context()

            await monitor.sweep(ctx)
            await monitor.sweep(ctx)  # a minute later, and the next, and the next

        empty_warnings = [
            call
            for call in logger.warning.call_args_list
            if "no tenants to examine" in call.args[0]
        ]

        # Said once, not once per tick — at a one-minute interval the second form would bury
        # the log of a legitimately idle replica.
        assert len(empty_warnings) == 1
        # And the answer is still fresh: an empty shard really has nothing stuck.
        assert monitor.scan_age() >= 0.0

    def test_a_registration_that_never_happened_does_not_consume_the_monitor(
        self, mocker: Any
    ) -> None:
        # The refusal above is what makes this one matter: taking the claim before the work
        # succeeds means an application missing the OTel extra burns its one set of gauges
        # on an attempt that registered nothing, and assembly cannot retry with a meter it
        # does supply.
        monitor = JobStalenessMonitor(silent_after=_WINDOW, spec=_SPEC)
        mocker.patch.dict(sys.modules, {"opentelemetry": None})

        with pytest.raises(ImportError):
            instrument_job_progress(monitor)

        mocker.stopall()
        meter = _CapturingMeter()
        instrument_job_progress(monitor, meter=meter)  # type: ignore[arg-type]

        assert JOBS_STALLED_GAUGE in meter.gauges

    async def test_a_configuration_error_stops_the_loop_loudly(self, mocker: Any) -> None:
        # A misrouted spec or a missing dep does not fix itself on the next tick, and a loop
        # that keeps failing every minute would bury the reason. It stops — and the scan age
        # is what tells an operator the numbers underneath it have stopped moving.
        runtime = _runtime()
        mocker.patch.object(
            JobStalenessMonitor, "sweep", side_effect=exc.configuration("no such route")
        )
        logger = mocker.patch.object(lifecycle_module, "logger")
        step, monitor = job_staleness_lifecycle_step(
            silent_after=_WINDOW, spec=_SPEC, interval=timedelta(milliseconds=5), jitter=0.0
        )

        async with runtime.scope():
            ctx = runtime.get_context()
            await step.startup(ctx)
            await _settle(step.startup.task)

            assert step.startup.task is not None
            assert step.startup.task.done()

            await step.shutdown(ctx)  # idempotent: the loop already returned

        assert monitor.scan_age() == -1.0  # it never completed a sweep, and says so
        assert any(
            "configuration error" in call.args[0] for call in logger.exception.call_args_list
        )

    @pytest.mark.parametrize(
        "failure",
        [exc.infrastructure("the store blinked"), RuntimeError("a bug in a projector")],
        ids=["backend", "defect"],
    )
    async def test_a_passing_failure_does_not_stop_the_loop(
        self, mocker: Any, failure: BaseException
    ) -> None:
        # The opposite of the rule above: a store that blinked, or a defect in one sweep, is
        # not a reason to stop watching for stuck jobs forever. It is logged and the next
        # tick asks again.
        runtime = _runtime()
        sweep = mocker.patch.object(JobStalenessMonitor, "sweep", side_effect=[failure, None, None])
        step, _ = job_staleness_lifecycle_step(
            silent_after=_WINDOW, spec=_SPEC, interval=timedelta(milliseconds=1), jitter=0.0
        )

        async with runtime.scope():
            ctx = runtime.get_context()
            await step.startup(ctx)

            for _ in range(200):
                await asyncio.sleep(0.002)

                if sweep.call_count >= 2:
                    break

            still_running = step.startup.task is not None and not step.startup.task.done()
            await step.shutdown(ctx)

        assert sweep.call_count >= 2  # it asked again after the failure
        assert still_running

    async def test_a_second_startup_does_not_start_a_second_loop(self, mocker: Any) -> None:
        # Two sweeps against one monitor would interleave their answers into one cache; the
        # duplicate is refused rather than silently doubling the reads.
        runtime = _runtime()
        logger = mocker.patch.object(lifecycle_module, "logger")
        step, _ = job_staleness_lifecycle_step(
            silent_after=_WINDOW, spec=_SPEC, interval=timedelta(seconds=30)
        )

        async with runtime.scope():
            ctx = runtime.get_context()
            await step.startup(ctx)
            first = step.startup.task

            await step.startup(ctx)

            assert step.startup.task is first
            assert step.startup.loop_name == "job_staleness"

            await step.shutdown(ctx)

        assert any("already running" in call.args[0] for call in logger.warning.call_args_list)

    async def test_a_sweep_cancelled_mid_flight_does_not_become_a_logged_failure(
        self, mocker: Any
    ) -> None:
        # Cancellation is the runtime asking this loop to stop — a deploy, a deadline, a
        # shutdown — not a sweep that failed. Swallowed by the generic handler it would be
        # logged as an error and the loop would carry on running through a drain.
        runtime = _runtime()
        logger = mocker.patch.object(lifecycle_module, "logger")

        async def _never_finishes(_ctx: Any) -> None:
            await asyncio.sleep(3600)

        mocker.patch.object(JobStalenessMonitor, "sweep", side_effect=_never_finishes)
        step, _ = job_staleness_lifecycle_step(
            silent_after=_WINDOW, spec=_SPEC, interval=timedelta(seconds=30)
        )

        async with runtime.scope():
            ctx = runtime.get_context()
            await step.startup(ctx)
            task = step.startup.task
            assert task is not None

            await asyncio.sleep(0)  # let the loop reach the sweep
            task.cancel()

            with pytest.raises(asyncio.CancelledError):
                await task

        assert not logger.exception.called

    async def test_a_loop_that_cannot_even_start_leaves_nothing_behind(self, mocker: Any) -> None:
        # The other half of the cleanup: if spawning the task fails, the control block must
        # come back armed-for-nothing rather than half-set, or a later startup would see a
        # loop that does not exist and decline to start one.
        runtime = _runtime()
        step, _ = job_staleness_lifecycle_step(
            silent_after=_WINDOW, spec=_SPEC, interval=timedelta(seconds=30)
        )

        def _refuse(coro: Any, **_kwargs: Any) -> None:
            coro.close()  # the loop never ran; closing it keeps the failure quiet

            raise RuntimeError("no event loop")

        async with runtime.scope():
            ctx = runtime.get_context()
            # Scoped to this one call: `create_task` is the event loop's, and leaving it
            # broken would take the runtime's own teardown with it.
            mocker.patch.object(lifecycle_module.asyncio, "create_task", side_effect=_refuse)

            with pytest.raises(RuntimeError):
                await step.startup(ctx)

            mocker.stopall()

            assert step.startup.task is None
            assert not step.startup.control.running

    async def test_a_loop_that_cannot_be_registered_is_not_left_running(self, mocker: Any) -> None:
        # The loop is only useful if the runtime can drain it. If registration fails, the
        # task it just spawned would otherwise outlive the failure — a sweep nothing owns,
        # still reading, unstoppable through the lifecycle.
        runtime = _runtime()
        step, _ = job_staleness_lifecycle_step(
            silent_after=_WINDOW, spec=_SPEC, interval=timedelta(seconds=30)
        )

        async with runtime.scope():
            ctx = runtime.get_context()
            mocker.patch.object(
                type(ctx.drainables), "register", side_effect=RuntimeError("registry closed")
            )

            with pytest.raises(RuntimeError):
                await step.startup(ctx)

            assert step.startup.task is None

    def test_a_nonsense_schedule_is_refused(self) -> None:
        with pytest.raises(CoreException):
            job_staleness_lifecycle_step(silent_after=_WINDOW, interval=timedelta(0))

        with pytest.raises(CoreException):
            job_staleness_lifecycle_step(silent_after=_WINDOW, jitter=1.0)
