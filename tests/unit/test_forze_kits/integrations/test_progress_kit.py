"""The operation-progress kit: reporter coalescing, and the projector's merge rules.

The load-bearing test here is :class:`TestAdversarialOrdering`, which replays every
permutation of one job's event sequence and asserts they all project to the same record.
That is the property the whole plane rests on — the tick lane is at-most-once and
unordered, resumed work re-reports old counters, and a durable transition relayed after a
crash can land behind ticks emitted long after it — and it is exactly the kind of logic a
code read clears wrongly, because each rule looks obviously correct in isolation and the
bugs live in their interaction.

The rest divides the same way the design does: what the *source* guarantees (the clamp and
the coalescing window, tested against a hand-driven clock so the assertions are about the
rule and not about how fast the test host is) and what the *store* guarantees (terminal
absorption, monotonic progress, a queryable staleness signal).
"""

from __future__ import annotations

import time
from datetime import UTC, datetime, timedelta
from functools import partial
from itertools import permutations
from typing import Any, Final
from uuid import UUID, uuid4

import attrs
import pytest
from pydantic import ValidationError

from forze.application.contracts.durable.function import DurableRunContext, bind_durable_run
from forze.application.contracts.inventory import (
    PlaneDisposition,
    SpecPlane,
    SpecRegistry,
    SpecSource,
)
from forze.application.contracts.realtime import Audience, RealtimeSignal
from forze.application.contracts.stream import StreamQueryDepKey
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import DepsRegistry, ExecutionContext, ExecutionRuntime
from forze.base.exceptions import CoreException, exc
from forze.base.primitives import FrozenTimeSource, bind_time_source, uuid7
from forze_kits.integrations.progress import reporter as reporter_module
from forze_kits.integrations.progress import (
    JOB_PROGRESS_EVENT_NAME,
    JobProgress,
    JobProgressProjector,
    JobRecord,
    JobStatus,
    ProgressReporter,
    build_job_progress_projector,
    build_progress_reporter,
    job_record_spec,
    job_topic,
    progress_outbox_spec,
    progress_spec_contributions,
)
from forze_kits.integrations.outbox import OutboxRelay
from forze_kits.integrations.outbox._relay_core import relay_outbox_claims
from forze_kits.integrations.realtime import realtime_outbox_spec, realtime_stream_spec
from forze_mock import MockDepsModule

# ----------------------- #

_SPEC: Final = job_record_spec()
_STREAM: Final = realtime_stream_spec()
_OUTBOX: Final = realtime_outbox_spec()
_PROGRESS_OUTBOX: Final = progress_outbox_spec()
_T0: Final = datetime(2026, 8, 4, 12, 0, 0, tzinfo=UTC)


def _runtime() -> ExecutionRuntime:
    return ExecutionRuntime(deps=DepsRegistry.from_modules(MockDepsModule()).freeze())


class _ManualTime:
    """A clock the test drives — wall *and* monotonic, so nothing depends on host speed."""

    def __init__(self, instant: datetime = _T0) -> None:
        self.instant = instant
        self.mono = 1_000.0

    def now(self) -> datetime:
        return self.instant

    def uuid(self) -> UUID:
        return uuid7(timestamp_ns=int(self.instant.timestamp() * 1_000_000_000))

    def monotonic(self) -> float:
        return self.mono

    def advance(self, seconds: float) -> None:
        self.instant += timedelta(seconds=seconds)
        self.mono += seconds


class _RecordingSink:
    """Captures what the reporter emitted and on which lane."""

    def __init__(self) -> None:
        self.emitted: list[tuple[JobProgress, bool]] = []

    async def emit(self, event: JobProgress, *, durable: bool) -> None:
        self.emitted.append((event, durable))

    @property
    def ticks(self) -> list[JobProgress]:
        return [event for event, durable in self.emitted if not durable]

    @property
    def transitions(self) -> list[JobProgress]:
        return [event for event, durable in self.emitted if durable]


class _FlakySink(_RecordingSink):
    """A sink that can be taken away and brought back — a broker blip, mid-job."""

    def __init__(self) -> None:
        super().__init__()
        self.down = False

    async def emit(self, event: JobProgress, *, durable: bool) -> None:
        if self.down:
            raise exc.infrastructure("sink unavailable")

        await super().emit(event, durable=durable)


class _RefusingSink:
    """A sink that is simply not there — the unreachable dashboard, the dead broker."""

    def __init__(self) -> None:
        self.seen = 0

    async def emit(self, event: JobProgress, *, durable: bool) -> None:
        self.seen += 1

        raise exc.infrastructure("sink unavailable")


def _reporter(*sinks: Any, min_interval: float = 0.0) -> ProgressReporter:
    return ProgressReporter(
        job_id=uuid4(),
        kind="export",
        sinks=tuple(sinks),
        min_interval=min_interval,
    )


def _event(
    job_id: UUID,
    status: JobStatus,
    *,
    seconds: float,
    seq: int,
    progress: float | None = None,
    message: str | None = None,
    error: str | None = None,
) -> JobProgress:
    return JobProgress(
        job_id=job_id,
        kind="export",
        status=status,
        at=_T0 + timedelta(seconds=seconds),
        seq=seq,
        progress=progress,
        message=message,
        error=error,
    )


def _observable(row: JobRecord) -> dict[str, Any]:
    """The record's meaning, without the fields write *order* legitimately moves."""

    return row.model_dump(exclude={"id", "rev", "created_at", "last_update_at"})


async def _find_job(ctx: ExecutionContext, job_id: UUID) -> JobRecord:
    row = await ctx.document.query(_SPEC).find({"$values": {"id": job_id}})
    assert row is not None

    return row


async def _apply_all(projector: JobProgressProjector, events: list[JobProgress]) -> JobRecord:
    for event in events:
        await projector.apply(event)

    row = await projector.query.find({"$values": {"id": events[0].job_id}})
    assert row is not None

    return row


# ----------------------- #


class TestAdversarialOrdering:
    """Every permutation of one job's events projects to the same record."""

    async def test_any_arrival_order_converges(self) -> None:
        # A whole job's life, deliberately including the shapes that make ordering matter:
        # a fraction that goes backwards (resumed work re-reporting), a pause and a resume,
        # and a terminal transition that is *not* the last event to be emitted.
        script = [
            (JobStatus.RUNNING, 0.0, 1, None, "starting"),
            (JobStatus.RUNNING, 1.0, 2, 0.25, "quarter"),
            (JobStatus.WAITING, 2.0, 3, 0.25, "waiting on an answer"),
            (JobStatus.RUNNING, 3.0, 4, 0.5, "resumed"),
            (JobStatus.RUNNING, 4.0, 5, 0.2, "a stale counter"),
            (JobStatus.SUCCEEDED, 5.0, 6, 1.0, "done"),
        ]

        runtime = _runtime()
        results: list[dict[str, Any]] = []

        async with runtime.scope():
            projector = build_job_progress_projector(runtime.get_context())

            for order in permutations(range(len(script))):
                job_id = uuid4()
                events = [
                    _event(
                        job_id,
                        script[i][0],
                        seconds=script[i][1],
                        seq=script[i][2],
                        progress=script[i][3],
                        message=script[i][4],
                    )
                    for i in order
                ]
                row = await _apply_all(projector, events)
                results.append(_observable(row))

        # One distinct outcome, whatever the transport did to the order.
        assert all(result == results[0] for result in results)
        assert results[0]["status"] is JobStatus.SUCCEEDED
        assert results[0]["progress"] == 1.0
        assert results[0]["message"] == "done"
        assert results[0]["started_at"] == _T0
        assert results[0]["finished_at"] == _T0 + timedelta(seconds=5)
        # The heartbeat is the newest report seen, not the newest *accepted* transition.
        assert results[0]["heartbeat_at"] == _T0 + timedelta(seconds=5)

    async def test_a_tick_after_the_end_changes_nothing(self) -> None:
        runtime = _runtime()

        async with runtime.scope():
            projector = build_job_progress_projector(runtime.get_context())
            job_id = uuid4()

            await projector.apply(_event(job_id, JobStatus.RUNNING, seconds=0, seq=1, progress=0.5))
            failed = await projector.apply(
                _event(job_id, JobStatus.FAILED, seconds=1, seq=2, error="boom")
            )
            assert failed is not None
            before = _observable(failed)

            # Everything a straggler could carry: a later timestamp, more progress, a
            # cheerful message. None of it may resurrect a job that ended.
            dropped = await projector.apply(
                _event(
                    job_id,
                    JobStatus.RUNNING,
                    seconds=9,
                    seq=99,
                    progress=0.9,
                    message="still going!",
                )
            )

            assert dropped is not None
            assert _observable(dropped) == before
            assert dropped.status is JobStatus.FAILED
            assert dropped.error == "boom"

    async def test_a_straggler_may_still_teach_when_the_job_started(self) -> None:
        # The one thing terminal absorption does not govern, and the reason the projection
        # converges at all: a row created by a terminal event that arrived first believes
        # the job started when it ended. That is the only fact a straggler may correct —
        # and it is a fact about the past, not a claim that the job is alive again.
        runtime = _runtime()

        async with runtime.scope():
            projector = build_job_progress_projector(runtime.get_context())
            job_id = uuid4()

            await projector.apply(
                _event(job_id, JobStatus.SUCCEEDED, seconds=5, seq=6, progress=1.0, message="done")
            )
            row = await projector.apply(
                _event(job_id, JobStatus.RUNNING, seconds=0, seq=1, message="starting")
            )

            assert row is not None
            assert row.started_at == _T0
            assert row.finished_at == _T0 + timedelta(seconds=5)
            # Everything else is untouched: no resurrection, no message rollback, no
            # heartbeat pretending the job reported after it ended.
            assert row.status is JobStatus.SUCCEEDED
            assert row.message == "done"
            assert row.heartbeat_at == _T0 + timedelta(seconds=5)

    async def test_a_late_terminal_wins_over_an_earlier_one(self) -> None:
        # Two sources ending the same job. They must converge, whichever lands first.
        runtime = _runtime()

        async with runtime.scope():
            projector = build_job_progress_projector(runtime.get_context())
            outcomes = []

            for order in ((0, 1), (1, 0)):
                job_id = uuid4()
                events = [
                    _event(job_id, JobStatus.SUCCEEDED, seconds=1, seq=1),
                    _event(job_id, JobStatus.FAILED, seconds=2, seq=1, error="late failure"),
                ]
                outcomes.append(
                    _observable(await _apply_all(projector, [events[i] for i in order]))
                )

            assert outcomes[0] == outcomes[1]
            assert outcomes[0]["status"] is JobStatus.FAILED

    async def test_waiting_is_reversible_within_one_instant(self) -> None:
        # A frozen clock stamps a whole burst with one timestamp, so the sequence is the
        # only thing that can order `waiting` before the `running` that resumes it. Without
        # it the resume would be silently dropped and a live job would read as paused.
        runtime = _runtime()

        async with runtime.scope():
            projector = build_job_progress_projector(runtime.get_context())
            job_id = uuid4()

            await projector.apply(_event(job_id, JobStatus.WAITING, seconds=0, seq=1))
            row = await projector.apply(_event(job_id, JobStatus.RUNNING, seconds=0, seq=2))

            assert row is not None
            assert row.status is JobStatus.RUNNING
            assert row.finished_at is None

    async def test_progress_survives_the_pause(self) -> None:
        # The terminate-and-resume grain: a fresh reporter (new run, sequence back to 1)
        # picks the task up where it stopped, and the bar does not reset.
        runtime = _runtime()

        async with runtime.scope():
            projector = build_job_progress_projector(runtime.get_context())
            job_id = uuid4()

            await projector.apply(_event(job_id, JobStatus.WAITING, seconds=0, seq=7, progress=0.6))
            row = await projector.apply(
                _event(job_id, JobStatus.RUNNING, seconds=60, seq=1, progress=None)
            )

            assert row is not None
            assert row.status is JobStatus.RUNNING
            assert row.progress == 0.6

    async def test_an_out_of_range_or_nan_fraction_cannot_poison_the_merge(self) -> None:
        runtime = _runtime()

        async with runtime.scope():
            projector = build_job_progress_projector(runtime.get_context())
            job_id = uuid4()

            await projector.apply(_event(job_id, JobStatus.RUNNING, seconds=0, seq=1, progress=0.4))
            # A NaN compares false against everything: accepted as the high-water mark it
            # would make every later comparison false and disable the monotonic rule.
            await projector.apply(
                _event(job_id, JobStatus.RUNNING, seconds=1, seq=2, progress=float("nan"))
            )
            row = await projector.apply(
                _event(job_id, JobStatus.RUNNING, seconds=2, seq=3, progress=4.2)
            )

            assert row is not None
            assert row.progress == 1.0

            after = await projector.apply(
                _event(job_id, JobStatus.RUNNING, seconds=3, seq=4, progress=0.1)
            )
            assert after is not None
            assert after.progress == 1.0


# ....................... #


class TestLateJoiner:
    async def test_an_unknown_job_creates_its_row(self) -> None:
        runtime = _runtime()

        async with runtime.scope():
            projector = build_job_progress_projector(runtime.get_context())
            job_id = uuid4()

            row = await projector.apply(
                _event(
                    job_id,
                    JobStatus.RUNNING,
                    seconds=5,
                    seq=42,
                    progress=0.5,
                    message="mid-sweep",
                )
            )

            assert row is not None
            assert row.id == job_id
            assert row.status is JobStatus.RUNNING
            assert row.progress == 0.5
            assert row.started_at == _T0 + timedelta(seconds=5)

    async def test_a_pending_report_never_un_starts_a_running_job(self) -> None:
        # Two processes, two clocks: whoever queues the job stamps `pending`, whoever runs it
        # stamps `running`, and a queueing service running a few seconds ahead produces a
        # `pending` that is *newer* than the report proving the job is executing. Taking the
        # newer one back would not merely show a wrong status — `pending` is not in the
        # staleness sweep's active set, so the job would also stop being watched at exactly
        # the moment it became able to hang.
        runtime = _runtime()

        async with runtime.scope():
            projector = build_job_progress_projector(runtime.get_context())
            job_id = uuid4()

            await projector.apply(_event(job_id, JobStatus.RUNNING, seconds=0, seq=1))
            row = await projector.apply(_event(job_id, JobStatus.PENDING, seconds=30, seq=1))

            assert row.status is JobStatus.RUNNING
            assert row.started_at == _T0
            # It is still a report, so the job is still heard from — only its *status* is
            # refused.
            assert row.heartbeat_at == _T0 + timedelta(seconds=30)

    async def test_a_late_running_report_supersedes_a_newer_pending_one(self) -> None:
        # The same pair the other way round, which is what keeps the rule order-independent:
        # a `running` that arrives after a newer `pending` must win too, or the two arrival
        # orders would disagree about a job that plainly started.
        runtime = _runtime()

        async with runtime.scope():
            projector = build_job_progress_projector(runtime.get_context())
            job_id = uuid4()

            await projector.apply(_event(job_id, JobStatus.PENDING, seconds=30, seq=1))
            row = await projector.apply(_event(job_id, JobStatus.RUNNING, seconds=0, seq=1))

            assert row.status is JobStatus.RUNNING
            assert row.started_at == _T0

    async def test_every_order_of_a_queued_job_converges(self) -> None:
        # The rule above is a *second* way for a status to lose, so it is a second way for
        # the projection to become order-dependent — the property everything else here rests
        # on. Same job, no terminal to absorb the disagreement, and the declaration stamped
        # by the clock that runs ahead: whichever order these arrive in, the record has to
        # be the same one.
        script = [
            (JobStatus.PENDING, 3.0, 1),
            (JobStatus.RUNNING, 0.0, 1),
            (JobStatus.WAITING, 2.0, 2),
        ]

        runtime = _runtime()
        results: list[dict[str, Any]] = []

        async with runtime.scope():
            projector = build_job_progress_projector(runtime.get_context())

            for order in permutations(range(len(script))):
                job_id = uuid4()
                results.append(
                    _observable(
                        await _apply_all(
                            projector,
                            [
                                _event(job_id, script[i][0], seconds=script[i][1], seq=script[i][2])
                                for i in order
                            ],
                        )
                    )
                )

        assert all(result == results[0] for result in results)
        assert results[0]["status"] is JobStatus.WAITING
        assert results[0]["started_at"] == _T0
        # Refused as a status, still counted as a report.
        assert results[0]["heartbeat_at"] == _T0 + timedelta(seconds=3)

    async def test_a_terminal_event_for_an_unknown_job_lands_finished(self) -> None:
        runtime = _runtime()

        async with runtime.scope():
            projector = build_job_progress_projector(runtime.get_context())
            job_id = uuid4()

            row = await projector.apply(
                _event(job_id, JobStatus.SUCCEEDED, seconds=5, seq=9, progress=1.0)
            )

            assert row is not None
            assert row.status is JobStatus.SUCCEEDED
            assert row.finished_at == _T0 + timedelta(seconds=5)


# ....................... #


class TestCoalescing:
    """The window bounds how *often* the transport hears, never which value it ends on."""

    async def test_a_burst_collapses_but_the_last_value_escapes(self) -> None:
        clock = _ManualTime()
        sink = _RecordingSink()

        with bind_time_source(clock):
            reporter = _reporter(sink, min_interval=0.3)

            for index in range(100):
                await reporter.report(index / 100, f"row {index}")

            # One tick reached the transport, not a hundred...
            assert len(sink.ticks) == 1
            assert sink.ticks[0].progress == 0.0

            # ...and the newest value is held, not dropped.
            await reporter.flush()

        assert len(sink.ticks) == 2
        assert sink.ticks[-1].progress == 0.99
        assert sink.ticks[-1].message == "row 99"

    async def test_the_window_reopens_with_time(self) -> None:
        clock = _ManualTime()
        sink = _RecordingSink()

        with bind_time_source(clock):
            reporter = _reporter(sink, min_interval=0.3)

            for index in range(10):
                await reporter.report(index / 10)
                clock.advance(0.1)

        # 1s of reporting at 10/s with a 300ms window: ~4 ticks, never 10.
        assert 3 <= len(sink.ticks) <= 4

    async def test_a_completed_bar_is_never_swallowed(self) -> None:
        # The failure this exists to prevent: report(1.0) as the last thing a job says,
        # coalesced into a window nothing ever flushes — a job stuck at 99% forever.
        clock = _ManualTime()
        sink = _RecordingSink()

        with bind_time_source(clock):
            reporter = _reporter(sink, min_interval=30.0)

            await reporter.report(0.5)
            await reporter.report(0.99)
            await reporter.report(1.0)

        assert [tick.progress for tick in sink.ticks] == [0.5, 1.0]

    async def test_a_transition_flushes_what_the_window_held(self) -> None:
        clock = _ManualTime()
        sink = _RecordingSink()

        with bind_time_source(clock):
            reporter = _reporter(sink, min_interval=30.0)

            await reporter.start()
            await reporter.report(0.4, "held")
            await reporter.fail("boom")

        # The held tick is emitted *before* the transition, so the last accepted message is
        # the failure's, not a stale one overtaking it.
        assert [event.status for event, _ in sink.emitted] == [
            JobStatus.RUNNING,
            JobStatus.RUNNING,
            JobStatus.FAILED,
        ]
        assert sink.emitted[1][0].message == "held"

    async def test_zero_interval_emits_every_report(self) -> None:
        clock = _ManualTime()
        sink = _RecordingSink()

        with bind_time_source(clock):
            reporter = _reporter(sink, min_interval=0.0)

            for index in range(5):
                await reporter.report(index / 10)

        assert len(sink.ticks) == 5

    async def test_a_heartbeat_says_alive_without_moving_the_bar(self) -> None:
        # What a job with nothing countable to report has: the staleness sweep reads
        # `heartbeat_at`, so a long indeterminate stretch has to be able to stay out of it
        # without inventing a fraction.
        sink = _RecordingSink()
        reporter = _reporter(sink, min_interval=0.0)

        await reporter.report(0.5, "halfway")
        await reporter.heartbeat("still working")

        assert reporter.progress == 0.5  # the high-water mark, unchanged
        assert sink.ticks[-1].progress == 0.5
        assert sink.ticks[-1].message == "still working"


# ....................... #


class TestSourceMonotonicity:
    async def test_a_fraction_below_the_high_water_mark_is_clamped(self) -> None:
        sink = _RecordingSink()
        reporter = _reporter(sink)

        await reporter.report(0.7)
        await reporter.report(0.2)  # a resumed worker re-printing an old counter

        assert [tick.progress for tick in sink.ticks] == [0.7, 0.7]

    async def test_advance_with_no_total_is_indeterminate(self) -> None:
        sink = _RecordingSink()
        reporter = _reporter(sink)

        await reporter.advance(0, 0)

        assert sink.ticks[0].progress is None

    async def test_advance_past_the_total_saturates(self) -> None:
        sink = _RecordingSink()
        reporter = _reporter(sink)

        await reporter.advance(11, 10)

        assert sink.ticks[0].progress == 1.0

    async def test_a_non_finite_fraction_is_refused(self) -> None:
        reporter = _reporter(_RecordingSink())

        with pytest.raises(CoreException) as err:
            await reporter.report(float("nan"))

        assert err.value.kind.value == "validation"

    async def test_finishing_completes_the_bar(self) -> None:
        sink = _RecordingSink()
        reporter = _reporter(sink)

        await reporter.report(0.97)
        await reporter.finish()

        assert sink.transitions[-1].progress == 1.0

    async def test_failing_keeps_the_last_value(self) -> None:
        sink = _RecordingSink()
        reporter = _reporter(sink)

        await reporter.report(0.4)
        await reporter.fail("boom")

        assert sink.transitions[-1].progress == 0.4

    async def test_a_terminal_job_cannot_report_again(self) -> None:
        reporter = _reporter(_RecordingSink())
        await reporter.finish()

        with pytest.raises(CoreException) as tick_err:
            await reporter.report(0.5)

        with pytest.raises(CoreException) as transition_err:
            await reporter.fail("boom")

        assert tick_err.value.kind.value == "precondition"
        assert transition_err.value.kind.value == "precondition"


# ....................... #


class TestTransportSplit:
    """Ticks ephemeral and droppable; transitions durable and not."""

    async def test_ticks_go_to_the_stream_and_transitions_to_the_outbox(self) -> None:
        runtime = _runtime()

        async with runtime.scope():
            ctx = runtime.get_context()
            job_id = uuid4()
            reporter = build_progress_reporter(
                ctx,
                job_id=job_id,
                kind="export",
                stream_spec=_STREAM,
                outbox_spec=_PROGRESS_OUTBOX,
                min_interval=0.0,
            )

            await reporter.start("go")
            await reporter.report(0.5, "half")
            await reporter.finish("done")

            relayed = await OutboxRelay(outbox_spec=_PROGRESS_OUTBOX).to_stream(ctx, _STREAM)
            query = ctx.deps.resolve_configurable(
                ctx, StreamQueryDepKey, _STREAM, route=str(_STREAM.name)
            )
            appended = await query.read({str(_STREAM.name): "0"})

        # The tick took the fire-and-forget lane straight to the stream; both transitions
        # went through the outbox and reached it only once the relay ran.
        assert [msg.payload.payload["status"] for msg in appended] == [
            JobStatus.RUNNING,  # the tick
            JobStatus.RUNNING,  # start, relayed
            JobStatus.SUCCEEDED,  # finish, relayed
        ]
        assert appended[0].payload.event == JOB_PROGRESS_EVENT_NAME
        assert appended[0].payload.payload["progress"] == 0.5
        assert appended[0].payload.audience == job_topic(job_id)
        assert relayed.published == 2

    async def test_every_transition_is_persisted_without_the_caller_flushing(self) -> None:
        # A job can pause across the end of the run that paused it, so a transition left
        # buffered for someone else's flush is a transition nobody ever sees. And staging
        # closes a route for the rest of the task, so the *second* transition is the one
        # that would go missing — a bug a test that only reports once cannot see.
        runtime = _runtime()

        async with runtime.scope():
            ctx = runtime.get_context()
            reporter = build_progress_reporter(
                ctx,
                job_id=uuid4(),
                kind="export",
                stream_spec=_STREAM,
                outbox_spec=_PROGRESS_OUTBOX,
                min_interval=0.0,
            )

            await reporter.start()
            await reporter.wait("needs an answer")  # the run could end right here

            relayed = await OutboxRelay(outbox_spec=_PROGRESS_OUTBOX).to_stream(ctx, _STREAM)

        assert relayed.published == 2

    async def test_transitions_can_be_relayed_to_a_queue_for_a_remote_projector(self) -> None:
        # The out-of-process recipe: the same dedicated route, pointed at a queue a consumer
        # runner drains. Which is a fork, not a tweak — transitions then reach the projector
        # and *not* the stream, so a UI on the stream sees ticks only.
        to_queue = progress_outbox_spec(queue="jobs")
        published: list[RealtimeSignal] = []
        runtime = _runtime()

        async def _deliver(claim: object, payload: object) -> None:
            assert isinstance(payload, RealtimeSignal)
            published.append(payload)

        async with runtime.scope():
            ctx = runtime.get_context()
            reporter = build_progress_reporter(
                ctx,
                job_id=uuid4(),
                kind="export",
                stream_spec=_STREAM,
                outbox_spec=to_queue,
                min_interval=0.0,
            )

            await reporter.start()
            await reporter.report(0.5)
            await reporter.finish()

            relayed = await relay_outbox_claims(
                ctx, outbox_spec=to_queue, publish_one=_deliver, reclaim_stale_after=None
            )

            projector = build_job_progress_projector(ctx)

            for signal in published:
                await projector.apply_signal(signal)

            row = await _find_job(ctx, reporter.job_id)

        assert to_queue.destination is not None
        assert to_queue.destination.kind == "queue"
        assert relayed.published == 2
        assert row.status is JobStatus.SUCCEEDED
        # The bar is the price: only transitions took this lane, so the record went from
        # nothing to done with the 0.5 tick nowhere in it.
        assert [signal.payload["progress"] for signal in published] == [None, 1.0]

    async def test_the_realtime_channel_s_own_outbox_route_is_refused(self) -> None:
        # `build_realtime_transport()` names its outbox route after the channel, so this is
        # the shape of handing over the application's shared route. Progress flushes after
        # every transition; on a shared route that persists whatever else was staged.
        runtime = _runtime()

        async with runtime.scope():
            with pytest.raises(CoreException) as err:
                build_progress_reporter(
                    runtime.get_context(),
                    job_id=uuid4(),
                    kind="export",
                    stream_spec=_STREAM,
                    outbox_spec=_OUTBOX,
                )

        assert err.value.code == "progress_reporter_shared_outbox"

    async def test_track_leaves_a_paused_job_paused(self) -> None:
        # The headline pattern: a terminate-and-resume run ends *cleanly* because it
        # succeeded at producing a question. A `track()` that finished the job here would
        # report the task as completed — the exact lie `waiting` exists to prevent, and the
        # one this plane was built to stop.
        sink = _RecordingSink()
        reporter = _reporter(sink)

        async with reporter.track("thinking"):
            await reporter.report(0.4, "needs a human answer")
            await reporter.wait("waiting on the operator")

        assert reporter.status is JobStatus.WAITING
        assert [event.status for event in sink.transitions] == [
            JobStatus.RUNNING,
            JobStatus.WAITING,
        ]
        assert sink.transitions[-1].message == "waiting on the operator"

    async def test_track_does_not_overwrite_an_outcome_the_block_stated(self) -> None:
        # A second transition would raise `progress_job_terminal` out of the `async with`,
        # punishing the caller who was most explicit about what happened.
        sink = _RecordingSink()
        reporter = _reporter(sink)

        async with reporter.track():
            await reporter.finish("done, explicitly")

        assert [event.status for event in sink.transitions] == [
            JobStatus.RUNNING,
            JobStatus.SUCCEEDED,
        ]
        assert sink.transitions[-1].message == "done, explicitly"

    async def test_track_keeps_the_failure_the_block_recorded(self) -> None:
        sink = _RecordingSink()
        reporter = _reporter(sink)

        with pytest.raises(RuntimeError):
            async with reporter.track():
                await reporter.fail("the disk filled up")

                raise RuntimeError("and then it propagated")

        # One failure, with the caller's own error text — not a second one describing the
        # exception that carried it out.
        assert [event.status for event in sink.transitions] == [
            JobStatus.RUNNING,
            JobStatus.FAILED,
        ]
        assert sink.transitions[-1].error == "the disk filled up"

    async def test_a_paused_job_that_then_raises_still_records_the_failure(self) -> None:
        # Terminal beats a pause (the projector's rule 5): a run that died on its way to
        # handing off did not hand off, and leaving it `waiting` would show a healthy pause.
        sink = _RecordingSink()
        reporter = _reporter(sink)

        with pytest.raises(RuntimeError):
            async with reporter.track():
                await reporter.wait("asking")

                raise RuntimeError("the question never got delivered")

        assert sink.transitions[-1].status is JobStatus.FAILED
        assert "the question never got delivered" in (sink.transitions[-1].error or "")

    async def test_a_failure_to_record_a_failure_never_replaces_it(self) -> None:
        # `track()` records the work's error on the way out, and that write can itself fail
        # — a cancelled sweep is cancelled at its first await, including this one. The
        # caller must still see their own exception, not the bookkeeping's.
        class _RefusesTheEnd:
            async def emit(self, event: JobProgress, *, durable: bool) -> None:
                if event.status is JobStatus.FAILED:
                    raise exc.infrastructure("the store went away too")

        reporter = _reporter(_RefusesTheEnd())

        with pytest.raises(RuntimeError, match="the disk filled up"):
            async with reporter.track():
                raise RuntimeError("the disk filled up")

    async def test_a_sink_that_raises_something_unexpected_still_does_not_kill_the_work(
        self,
    ) -> None:
        # A sink raising a plain `TypeError` is a defect in that sink — and still not worth
        # a six-hour export. The rule is about the lane, not about which exception type the
        # sink happened to pick.
        class _Buggy:
            async def emit(self, event: JobProgress, *, durable: bool) -> None:
                raise TypeError("a bug in somebody's sink")

        recorder = _RecordingSink()
        reporter = _reporter(_Buggy(), recorder)

        await reporter.report(0.5)  # absorbed; the healthy sink still gets it

        with pytest.raises(TypeError):
            await reporter.finish()  # the transition is not absorbed

        assert [tick.progress for tick in recorder.ticks] == [0.5]

    async def test_a_refusing_sink_drops_ticks_but_fails_transitions(self) -> None:
        # Observability must not kill the work it observes — but the event whose loss
        # leaves a job wrong forever is not one to swallow.
        sink = _RefusingSink()
        reporter = _reporter(sink)

        await reporter.report(0.5)  # absorbed

        with pytest.raises(CoreException):
            await reporter.finish()

        assert sink.seen == 2

    async def test_a_signal_for_another_event_is_ignored(self) -> None:
        runtime = _runtime()

        async with runtime.scope():
            projector = build_job_progress_projector(runtime.get_context())

            assert (
                await projector.apply_signal(
                    RealtimeSignal.of(Audience.topic("chat"), "message.new", {"text": "hi"})
                )
                is None
            )

    async def test_a_progress_signal_projects(self) -> None:
        runtime = _runtime()

        async with runtime.scope():
            projector = build_job_progress_projector(runtime.get_context())
            job_id = uuid4()
            event = _event(job_id, JobStatus.RUNNING, seconds=0, seq=1, progress=0.5)

            row = await projector.apply_signal(
                RealtimeSignal.of(
                    job_topic(job_id), JOB_PROGRESS_EVENT_NAME, event.model_dump(mode="json")
                )
            )

            assert row is not None
            assert row.progress == 0.5


# ....................... #


class TestRelayCrashRecovery:
    """Kill the relay mid-run: ticks may vanish, transitions must not.

    This is the half of the ephemeral/durable split that cannot be checked on the happy path.
    A relay pass that publishes nothing because the broker is down is the same state a worker
    that simply died leaves behind — rows claimed, nothing delivered — and what has to be true
    afterwards is that the *record* is still reachable from the durable half alone.
    """

    async def test_transitions_survive_a_dead_broker_and_need_no_tick(self) -> None:
        delivered: list[RealtimeSignal] = []
        runtime = _runtime()

        async def _broker_is_down(claim: object, payload: object) -> None:
            raise RuntimeError("broker unreachable")

        async def _record_delivery(claim: object, payload: object) -> None:
            assert isinstance(payload, RealtimeSignal)
            delivered.append(payload)

        async with runtime.scope():
            ctx = runtime.get_context()
            job_id = uuid4()
            reporter = build_progress_reporter(
                ctx,
                job_id=job_id,
                kind="export",
                stream_spec=_STREAM,
                outbox_spec=_PROGRESS_OUTBOX,
                min_interval=0.0,
            )
            relay = partial(
                relay_outbox_claims,
                ctx,
                outbox_spec=_PROGRESS_OUTBOX,
                reclaim_stale_after=None,
                retry_base_delay=timedelta(seconds=1),
                retry_max_backoff=timedelta(seconds=60),
            )

            with bind_time_source(FrozenTimeSource(instant=_T0)):
                await reporter.start("go")
                await reporter.report(0.5, "half")  # ephemeral — never staged
                await reporter.finish("done")

                crashed = await relay(publish_one=_broker_is_down)

            # Nothing was delivered, and nothing was lost: the transitions are still owed.
            assert crashed.published == 0
            assert crashed.retried == 2

            with bind_time_source(FrozenTimeSource(instant=_T0 + timedelta(minutes=5))):
                recovered = await relay(publish_one=_record_delivery)

            assert recovered.published == 2

            # Only transitions took this lane; the tick is not in it and never will be.
            assert [signal.payload["status"] for signal in delivered] == [
                JobStatus.RUNNING,
                JobStatus.SUCCEEDED,
            ]

            projector = build_job_progress_projector(ctx)

            for signal in delivered:
                await projector.apply_signal(signal)

            once = _observable(await _find_job(ctx, job_id))

            # At-least-once: a relay that died between publishing and marking republishes on
            # recovery, so the consumer sees every transition twice. The merge key makes that
            # a no-op rather than a second, contradictory story about the same job.
            for signal in delivered:
                await projector.apply_signal(signal)

            twice = _observable(await _find_job(ctx, job_id))

        # The record is complete from the durable half alone — no tick survived, and none had
        # to: losing every one of them costs the bar's intermediate values, nothing else.
        assert once["status"] is JobStatus.SUCCEEDED
        assert once["progress"] == 1.0
        assert once["message"] == "done"
        assert once["started_at"] == _T0
        assert once["finished_at"] == _T0
        assert twice == once


# ....................... #


class TestThePayloadBoundary:
    """What the projector is allowed to assume, enforced where a foreign payload arrives."""

    def test_a_naive_instant_is_refused(self) -> None:
        # The merge compares this against the record's stored instants, and Python raises on
        # naive-vs-aware. A producer in another language that serialized a local timestamp
        # without an offset would not merge wrong — it would raise `TypeError` out of the
        # middle of the projector and leave the job unprojected, with the payload long gone
        # from the traceback. Refused here, where it is still identifiable.
        with pytest.raises(ValidationError):
            JobProgress(
                job_id=uuid4(),
                kind="export",
                status=JobStatus.RUNNING,
                at=datetime(2026, 8, 4, 12, 0, 0),  # noqa: DTZ001 — the point of the test
                seq=1,
            )

    def test_an_error_without_a_failure_is_refused(self) -> None:
        # The projector copies `error` onto the record with every status it accepts, so this
        # would otherwise produce a row that says `running` and explains why it failed —
        # and, once the job succeeded, a success with a reason.
        with pytest.raises(ValidationError):
            JobProgress(
                job_id=uuid4(),
                kind="export",
                status=JobStatus.SUCCEEDED,
                at=_T0,
                seq=1,
                error="but it worked",
            )

    def test_a_failure_carries_its_reason(self) -> None:
        event = JobProgress(
            job_id=uuid4(),
            kind="export",
            status=JobStatus.FAILED,
            at=_T0,
            seq=1,
            error="the disk filled up",
        )

        assert event.error == "the disk filled up"


# ....................... #


class _IdentityWatchingProjector:
    """Records the tenant that was ambient when the record was actually written.

    Handed to :func:`build_progress_reporter` as the merger, so what it sees is what the
    *wiring* produced — a sink wrapped by hand would only prove the wrapper works.
    """

    def __init__(self, ctx: ExecutionContext) -> None:
        self.ctx = ctx
        self.seen: list[UUID | None] = []

    async def apply(self, event: JobProgress) -> object:
        tenant = self.ctx.inv_ctx.get_tenant()
        self.seen.append(tenant.tenant_id if tenant is not None else None)

        return None


class TestTheJobsIdentity:
    """A job belongs to the identity that opened it, not to whatever is ambient at emit."""

    async def test_a_tick_is_written_under_the_identity_that_opened_the_job(self) -> None:
        # The failure this closes is silent and produces the worst possible record. The work
        # a job watches routinely runs under bindings of its own — a full-system export
        # walks one bound section per tenant, a per-tenant reindex is the same call under
        # each — and every port under a sink decides where a write lands by reading the
        # ambient tenant *at write time*. Unbound, one job scatters a row into every
        # partition its work touched, each stuck at whatever status was ambient there.
        opener = uuid4()
        elsewhere = uuid4()
        runtime = _runtime()

        async with runtime.scope():
            ctx = runtime.get_context()
            watching = _IdentityWatchingProjector(ctx)

            with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=opener)):
                reporter = build_progress_reporter(
                    ctx,
                    job_id=uuid4(),
                    kind="export",
                    projector=watching,
                    min_interval=0.0,
                )
                await reporter.start()

            # The work moves on to another tenant's data — the export's next section.
            with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=elsewhere)):
                await reporter.report(0.5, "halfway")
                await reporter.finish("done")

        assert watching.seen == [opener, opener, opener]

    async def test_a_held_tick_is_written_under_the_job_not_the_flusher(self) -> None:
        # Coalescing widens the same crack: a value captured under one binding is emitted by
        # whatever comes *next*, which can be under another one entirely.
        opener = uuid4()
        elsewhere = uuid4()
        clock = _ManualTime()
        runtime = _runtime()

        with bind_time_source(clock):
            async with runtime.scope():
                ctx = runtime.get_context()
                watching = _IdentityWatchingProjector(ctx)

                with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=opener)):
                    reporter = build_progress_reporter(
                        ctx,
                        job_id=uuid4(),
                        kind="export",
                        projector=watching,
                        min_interval=5.0,
                    )
                    await reporter.start()
                    await reporter.report(0.5, "held by the window")

                with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=elsewhere)):
                    await reporter.flush()

        assert watching.seen == [opener, opener]


# ....................... #


class _BlindQuery:
    """A query that cannot see the row — the shape a misrouted or mis-tenanted port has."""

    def __init__(self, inner: Any, *, blind_for: int) -> None:
        self.inner = inner
        self.blind_for = blind_for

    async def find(self, *args: Any, **kwargs: Any) -> Any:
        if self.blind_for > 0:
            self.blind_for -= 1

            return None

        return await self.inner.find(*args, **kwargs)


class _FailingCommand:
    """A command that raises *failure* on its first *times* calls, then behaves."""

    def __init__(self, inner: Any, *, failure: BaseException, times: int, on: str) -> None:
        self.inner = inner
        self.failure = failure
        self.times = times
        self.on = on

    async def _maybe_fail(self, name: str) -> None:
        if name == self.on and self.times > 0:
            self.times -= 1

            raise self.failure

    async def create(self, *args: Any, **kwargs: Any) -> Any:
        await self._maybe_fail("create")

        return await self.inner.create(*args, **kwargs)

    async def update(self, *args: Any, **kwargs: Any) -> Any:
        await self._maybe_fail("update")

        return await self.inner.update(*args, **kwargs)


def _projector(ctx: ExecutionContext, *, query: Any = None, command: Any = None) -> Any:
    built = build_job_progress_projector(ctx)

    return attrs.evolve(
        built,
        query=query if query is not None else built.query,
        command=command if command is not None else built.command,
    )


class TestTheMergeUnderConcurrency:
    """The CAS loop: two reporters, one row, and the ways a store says "someone else won"."""

    async def test_a_lost_insert_race_merges_onto_the_winner(self) -> None:
        # Two first events for one job arrive at once: both see no row, both insert, one
        # loses. The loser must not raise and must not lose its report — it re-reads and
        # merges onto the row the winner created.
        runtime = _runtime()

        async with runtime.scope():
            ctx = runtime.get_context()
            job_id = uuid4()

            await _projector(ctx).apply(_event(job_id, JobStatus.RUNNING, seconds=0, seq=1))

            # Blind for one read: it will try to insert, conflict, and look again.
            loser = _projector(ctx, query=_BlindQuery(ctx.document.query(_SPEC), blind_for=1))
            row = await loser.apply(
                _event(job_id, JobStatus.RUNNING, seconds=1, seq=2, progress=0.5)
            )

        assert row.progress == 0.5
        assert row.rev == 2  # merged onto the winner's row, not a second row

    async def test_a_lost_update_race_retries(self) -> None:
        # The store rejecting a stale revision is the ordinary outcome of two projectors
        # merging one job, not an error to surface: re-read, re-merge, write again.
        runtime = _runtime()

        async with runtime.scope():
            ctx = runtime.get_context()
            job_id = uuid4()

            await _projector(ctx).apply(_event(job_id, JobStatus.RUNNING, seconds=0, seq=1))

            contended = _projector(
                ctx,
                command=_FailingCommand(
                    ctx.document.command(_SPEC),
                    failure=exc.concurrency("someone else bumped the rev"),
                    times=1,
                    on="update",
                ),
            )
            row = await contended.apply(
                _event(job_id, JobStatus.SUCCEEDED, seconds=1, seq=2, message="done")
            )

        assert row.status is JobStatus.SUCCEEDED
        assert row.message == "done"

    @pytest.mark.parametrize("failing", ["create", "update"], ids=["insert", "merge"])
    async def test_a_store_failure_is_not_swallowed_as_contention(self, failing: str) -> None:
        # Only a revision conflict means "try again". Anything else is the store being
        # broken, and a merge loop that retried it would spin eight times and then report a
        # convergence problem that never existed.
        runtime = _runtime()

        async with runtime.scope():
            ctx = runtime.get_context()
            job_id = uuid4()

            if failing == "update":
                await _projector(ctx).apply(_event(job_id, JobStatus.RUNNING, seconds=0, seq=1))

            projector = _projector(
                ctx,
                command=_FailingCommand(
                    ctx.document.command(_SPEC),
                    failure=exc.infrastructure("the store is down"),
                    times=1,
                    on=failing,
                ),
            )

            with pytest.raises(CoreException) as err:
                await projector.apply(_event(job_id, JobStatus.RUNNING, seconds=1, seq=2))

        assert err.value.kind.value == "infrastructure"

    async def test_a_row_that_conflicts_but_cannot_be_read_gives_up_and_says_why(self) -> None:
        # The unwinnable shape: the id conflicts on insert, so a row exists — and this scope
        # cannot see it, so there is nothing to merge onto. Retrying forever would hang the
        # consumer; the exception names the cause, because it is nearly always the job
        # collection's tenancy wiring rather than contention.
        runtime = _runtime()

        async with runtime.scope():
            ctx = runtime.get_context()
            job_id = uuid4()

            await _projector(ctx).apply(_event(job_id, JobStatus.RUNNING, seconds=0, seq=1))

            blind = _projector(ctx, query=_BlindQuery(ctx.document.query(_SPEC), blind_for=1_000))

            with pytest.raises(CoreException) as err:
                await blind.apply(_event(job_id, JobStatus.RUNNING, seconds=1, seq=2))

        assert err.value.code == "job_progress_merge_stalled"


# ....................... #


class TestSpecContributions:
    """The inventory half — both progress specs are ones no application author wrote."""

    def test_both_halves_are_catalogued_as_the_kit_s(self) -> None:
        entries = progress_spec_contributions(spec=_SPEC, outbox_spec=_PROGRESS_OUTBOX).freeze()
        by_plane = {
            entry.plane: (entry.name, entry.disposition, entry.source) for entry in entries.entries
        }

        # The job collection is system of record — nothing recomputes the history of what ran
        # — while the transitions route is in-flight work a quiesce drains like any outbox.
        assert by_plane[SpecPlane.DOCUMENT] == (
            str(_SPEC.name),
            PlaneDisposition.EXPORTABLE,
            SpecSource.KIT,
        )
        assert by_plane[SpecPlane.OUTBOX] == (
            str(_PROGRESS_OUTBOX.name),
            PlaneDisposition.DRAINED,
            SpecSource.KIT,
        )

    def test_only_the_half_you_wired_is_catalogued(self) -> None:
        # A catalogued route nothing binds fails startup, so an application that keeps the
        # record without the realtime lane must not have the outbox route registered for it.
        entries = progress_spec_contributions(spec=_SPEC).freeze()

        assert [entry.plane for entry in entries.entries] == [SpecPlane.DOCUMENT]

    def test_only_the_outbox_half_is_catalogued_when_that_is_all_you_wired(self) -> None:
        # The mirror of the case above: a deployment projecting from a consumer wires the
        # route and not the collection, and cataloguing a spec nothing binds fails startup.
        entries = progress_spec_contributions(outbox_spec=_PROGRESS_OUTBOX).freeze()

        assert [entry.plane for entry in entries.entries] == [SpecPlane.OUTBOX]

    def test_contributing_nothing_is_refused(self) -> None:
        with pytest.raises(CoreException) as err:
            progress_spec_contributions()

        assert err.value.code == "progress_spec_contributions_empty"

    def test_the_contribution_merges_into_an_application_s_inventory(self) -> None:
        # The assembly-time shape: the app's own specs plus the kit's, under one registry.
        registry = (
            SpecRegistry()
            .register(realtime_stream_spec())
            .merge(progress_spec_contributions(spec=_SPEC, outbox_spec=_PROGRESS_OUTBOX))
            .freeze()
        )

        assert {entry.name for entry in registry.entries} == {
            str(_STREAM.name),
            str(_SPEC.name),
            str(_PROGRESS_OUTBOX.name),
        }


# ....................... #


class TestStaleness:
    async def test_stuck_jobs_are_findable_by_heartbeat(self) -> None:
        runtime = _runtime()

        async with runtime.scope():
            projector = build_job_progress_projector(runtime.get_context())
            stuck, alive, done, paused = (uuid4() for _ in range(4))

            await projector.apply(_event(stuck, JobStatus.RUNNING, seconds=0, seq=1))
            await projector.apply(_event(paused, JobStatus.WAITING, seconds=1, seq=1))
            await projector.apply(_event(alive, JobStatus.RUNNING, seconds=500, seq=1))
            await projector.apply(_event(done, JobStatus.SUCCEEDED, seconds=0, seq=1))

            stalled = await projector.find_stalled(silent_since=_T0 + timedelta(seconds=60))

        # A finished job is not stuck, however long ago it finished; a paused one still is
        # (nothing has reported for it, and `waiting` is not an excuse to stop watching).
        assert [row.id for row in stalled] == [stuck, paused]

    async def test_staleness_filters_by_kind(self) -> None:
        runtime = _runtime()

        async with runtime.scope():
            projector = build_job_progress_projector(runtime.get_context())
            job_id = uuid4()

            await projector.apply(_event(job_id, JobStatus.RUNNING, seconds=0, seq=1))

            assert not await projector.find_stalled(
                silent_since=_T0 + timedelta(seconds=60), kind="reencrypt"
            )
            assert await projector.find_stalled(
                silent_since=_T0 + timedelta(seconds=60), kind="export"
            )


# ....................... #


class TestWiringGuards:
    def test_a_reporter_with_no_sinks_is_refused(self) -> None:
        with pytest.raises(CoreException) as err:
            ProgressReporter(job_id=uuid4(), kind="export", sinks=())

        assert err.value.kind.value == "configuration"

    def test_a_negative_window_is_refused(self) -> None:
        # A negative window is not "no throttling" — `_within_window` would compare against
        # a time in the past and the coalescer would silently become a plain emitter.
        with pytest.raises(CoreException) as err:
            _reporter(_RecordingSink(), min_interval=-1.0)

        assert err.value.kind.value == "configuration"

    async def test_a_realtime_reporter_without_an_outbox_is_refused(self) -> None:
        # It would publish ticks happily and then be unable to say the job finished.
        runtime = _runtime()

        async with runtime.scope():
            with pytest.raises(CoreException) as err:
                build_progress_reporter(
                    runtime.get_context(),
                    job_id=uuid4(),
                    kind="export",
                    stream_spec=_STREAM,
                )

        assert err.value.code == "progress_reporter_no_outbox"

    async def test_a_projector_is_refused_in_a_read_only_operation(self) -> None:
        runtime = _runtime()

        async with runtime.scope():
            ctx = runtime.get_context()

            with ctx.inv_ctx.bind_read_only(), pytest.raises(CoreException) as err:
                build_job_progress_projector(ctx)

        assert err.value.kind.value == "precondition"

    def test_sealing_the_query_surface_is_refused(self) -> None:
        from forze.application.contracts.crypto import FieldEncryption

        with pytest.raises(CoreException) as err:
            job_record_spec(encryption=FieldEncryption(encrypted=frozenset({"heartbeat_at"})))

        assert err.value.code == "job_record_sealed_index"

    def test_sealing_the_human_fields_is_allowed(self) -> None:
        from forze.application.contracts.crypto import FieldEncryption

        spec = job_record_spec(encryption=FieldEncryption(encrypted=frozenset({"message"})))

        assert spec.encryption is not None


# ....................... #


class TestDurableLinkage:
    async def test_a_reporter_built_inside_a_run_links_to_it(self) -> None:
        runtime = _runtime()
        run = DurableRunContext(run_id=str(uuid7()), name="export", attempt=1)

        async with runtime.scope():
            ctx = runtime.get_context()
            projector = build_job_progress_projector(ctx)
            job_id = uuid4()

            token = bind_durable_run(run)

            try:
                reporter = build_progress_reporter(
                    ctx, job_id=job_id, kind="export", projector=projector, min_interval=0.0
                )
                await reporter.start()

            finally:
                from forze.application.contracts.durable.function import reset_durable_run

                reset_durable_run(token)

            row = await ctx.document.query(_SPEC).find({"$values": {"id": job_id}})

        assert reporter.durable_run_id == run.run_id
        assert row is not None
        assert row.durable_run_id == run.run_id

    async def test_a_run_id_learned_later_fills_in(self) -> None:
        # A job can outlive the run that produced it (that is what `waiting` is for), so the
        # link often arrives on a later report than the one that created the row. It fills
        # in rather than being ignored — and a tick that omits it is not asserting the job
        # has no run.
        runtime = _runtime()
        run_id = str(uuid7())

        async with runtime.scope():
            projector = build_job_progress_projector(runtime.get_context())
            job_id = uuid4()

            await projector.apply(_event(job_id, JobStatus.RUNNING, seconds=0, seq=1))
            row = await projector.apply(
                JobProgress(
                    job_id=job_id,
                    kind="export",
                    status=JobStatus.RUNNING,
                    at=_T0 + timedelta(seconds=1),
                    seq=2,
                    subject="acme",
                    durable_run_id=run_id,
                )
            )
            kept = await projector.apply(_event(job_id, JobStatus.RUNNING, seconds=2, seq=3))

        assert row.durable_run_id == run_id
        assert row.subject == "acme"
        assert kept.durable_run_id == run_id  # a later tick that omits it says nothing
        assert kept.subject == "acme"

    async def test_a_reporter_built_outside_a_run_links_to_nothing(self) -> None:
        runtime = _runtime()

        async with runtime.scope():
            ctx = runtime.get_context()
            reporter = build_progress_reporter(
                ctx,
                job_id=uuid4(),
                kind="export",
                projector=build_job_progress_projector(ctx),
            )

        assert reporter.durable_run_id is None


# ....................... #


class TestInlineRecording:
    async def test_the_record_follows_the_work(self) -> None:
        runtime = _runtime()

        async with runtime.scope():
            ctx = runtime.get_context()
            projector = build_job_progress_projector(ctx)
            job_id = uuid4()
            reporter = build_progress_reporter(
                ctx,
                job_id=job_id,
                kind="export",
                subject="acme",
                projector=projector,
                min_interval=0.0,
            )

            await reporter.start("starting")
            await reporter.advance(2, 4, "halfway")
            await reporter.wait("needs an answer")
            await reporter.start("resumed")
            await reporter.finish("done")

            row = await ctx.document.query(_SPEC).find({"$values": {"id": job_id}})

        assert row is not None
        assert row.status is JobStatus.SUCCEEDED
        assert row.progress == 1.0
        assert row.message == "done"
        assert row.subject == "acme"
        assert row.started_at is not None
        assert row.finished_at is not None

    async def test_a_block_that_raises_after_succeeding_keeps_its_own_word(
        self, mocker: Any
    ) -> None:
        # Both facts are real and only one can be in the record: the job stated success, and
        # then the block failed on its way out. The stated outcome stands — a second
        # transition would raise out of the `async with` and punish the most explicit
        # caller — but it is worth saying out loud, because the record and the traceback
        # now disagree and only the log connects them.
        logger = mocker.patch.object(reporter_module, "logger")
        sink = _RecordingSink()
        reporter = _reporter(sink, min_interval=0.0)

        with pytest.raises(RuntimeError):
            async with reporter.track():
                await reporter.finish("done")

                raise RuntimeError("the cleanup blew up")

        assert [event.status for event in sink.transitions] == [
            JobStatus.RUNNING,
            JobStatus.SUCCEEDED,
        ]
        assert any(
            "raised after reporting success" in call.args[0]
            for call in logger.warning.call_args_list
        )

    async def test_a_failed_finish_does_not_leave_the_bar_at_the_end(self) -> None:
        # `finish()` completes the bar before emitting, because the terminal event carries
        # the fraction. If that emit fails the job is still running — and a 1.0 high-water
        # mark left behind clamps every later report up to it, so the work carries on under
        # a bar that says it is done.
        flaky = _FlakySink()
        reporter = _reporter(flaky, min_interval=0.0)

        await reporter.start()
        await reporter.report(0.5, "halfway")
        flaky.down = True

        with pytest.raises(CoreException):
            await reporter.finish("done")

        flaky.down = False
        assert reporter.progress == 0.5

        await reporter.report(0.6, "still going")

        assert flaky.ticks[-1].progress == 0.6

    async def test_a_failed_terminal_can_be_stated_again(self) -> None:
        # The strand this closes: the local status was committed before the durable emit
        # that publishes it, so an outbox that was down for one call left a reporter that
        # believed the job had finished and a record that never heard — and the terminal
        # guard then refused every retry. A job stuck at `running` forever, arrived at by
        # way of reporting that it wasn't.
        runtime = _runtime()
        refusing = _RefusingSink()
        recording = _RecordingSink()

        async with runtime.scope():
            ctx = runtime.get_context()
            projector = build_job_progress_projector(ctx)
            job_id = uuid4()
            reporter = build_progress_reporter(
                ctx,
                job_id=job_id,
                kind="export",
                projector=projector,
                min_interval=0.0,
            )
            await reporter.start()

            # The sink goes away *after* the job is running — the shape a real outage has.
            reporter.sinks = (*reporter.sinks, refusing)

            with pytest.raises(CoreException):
                await reporter.finish("done")

            # Not stranded: the sink comes back and the caller states the outcome again.
            reporter.sinks = (*reporter.sinks[:-1], recording)
            await reporter.finish("done, on the second attempt")

            row = await _find_job(ctx, job_id)

        assert row.status is JobStatus.SUCCEEDED
        assert row.message == "done, on the second attempt"
        # A fresh sequence for the retry: the first event may have reached some sink, and
        # two different events sharing one number would break the merge's tie-break.
        assert [event.seq for event in recording.transitions] == [3]

    async def test_a_terminal_nothing_accepted_leaves_the_job_reportable(self) -> None:
        # The other half, with one sink so nothing partially succeeded: a transition that
        # reached nobody must leave the reporter exactly as it was. A committed status would
        # have raised `progress_job_terminal` on the very next tick — taking down the work
        # that was still running, over a report about it.
        flaky = _FlakySink()
        reporter = _reporter(flaky, min_interval=0.0)

        await reporter.start()
        flaky.down = True

        with pytest.raises(CoreException):
            await reporter.fail("the disk filled up")

        assert reporter.status is JobStatus.RUNNING

        flaky.down = False
        await reporter.report(0.5, "still going")
        await reporter.finish("done after all")

        assert [event.status for event in flaky.transitions] == [
            JobStatus.RUNNING,
            JobStatus.SUCCEEDED,
        ]
        assert flaky.ticks[-1].progress == 0.5

    async def test_the_clock_is_the_seam_the_record_is_stamped_from(self) -> None:
        # DST-safety: nothing in the reporter or the projector reads the system clock, so a
        # bound time source fixes every timestamp the record carries.
        clock = _ManualTime()
        runtime = _runtime()

        with bind_time_source(clock):
            async with runtime.scope():
                ctx = runtime.get_context()
                job_id = uuid4()
                reporter = build_progress_reporter(
                    ctx,
                    job_id=job_id,
                    kind="export",
                    projector=build_job_progress_projector(ctx),
                    min_interval=0.0,
                )

                await reporter.start()
                clock.advance(90)
                await reporter.finish()

                row = await ctx.document.query(_SPEC).find({"$values": {"id": job_id}})

        assert row is not None
        assert row.started_at == _T0
        assert row.finished_at == _T0 + timedelta(seconds=90)
        assert row.heartbeat_at == _T0 + timedelta(seconds=90)


# ....................... #


def test_the_manual_clock_is_not_the_system_clock() -> None:
    # Guards the tests above: if `bind_time_source` stopped reaching the kit, the
    # coalescing assertions would silently start measuring the host's speed.
    clock = _ManualTime()
    before = time.monotonic()

    with bind_time_source(clock):
        from forze.base.primitives import monotonic, utcnow

        assert utcnow() == _T0
        assert monotonic() == clock.mono

    assert time.monotonic() >= before
