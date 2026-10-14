"""Closing many connections over a window, and the drain phase that fits it to its budget."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Sequence
from datetime import timedelta

import pytest

from forze.application.contracts.execution import LifecycleStep
from forze.application.execution import build_runtime
from forze.application.execution.background import spread_closes, spread_drain_lifecycle_step
from forze.application.execution.background.spread import CloseFn
from forze.base.exceptions import CoreException, ExceptionKind

# ----------------------- #

SLACK = 0.25
"""Real-time tolerance for a loaded runner; every bound below is far from its alternative."""


def _slow_closes(count: int, *, takes: float, starts: list[float]) -> list[CloseFn]:
    clock = asyncio.get_running_loop()

    def close_for(index: int) -> CloseFn:
        async def close() -> None:
            starts[index] = clock.time()
            await asyncio.sleep(takes)

        return close

    return [close_for(index) for index in range(count)]


# ----------------------- #


class TestSpreadCloses:
    async def test_thousands_of_slow_closes_finish_within_window_plus_tail(self) -> None:
        count, window, tail, takes = 3_500, 0.5, 0.3, 0.05
        starts = [0.0] * count
        clock = asyncio.get_running_loop()
        began = clock.time()

        outcome = await spread_closes(
            _slow_closes(count, takes=takes, starts=starts),
            timedelta(seconds=window),
            tail=timedelta(seconds=tail),
        )
        elapsed = clock.time() - began

        # Awaited in turn they would take count * takes = 175 s.
        assert outcome.closed == count
        assert (outcome.failed, outcome.timed_out) == (0, 0)
        assert elapsed <= window + tail + SLACK

        # Close i starts at i * window / n: spread evenly, not bunched at either end.
        offsets = [start - began for start in starts]
        assert offsets == sorted(offsets)

        for index in (0, count // 4, count // 2, 3 * count // 4, count - 1):
            assert offsets[index] == pytest.approx(index * window / count, abs=0.1)

    # ....................... #

    async def test_a_close_still_running_after_the_tail_is_cancelled(self) -> None:
        cancelled = asyncio.Event()

        async def stuck() -> None:
            try:
                await asyncio.sleep(60)

            except asyncio.CancelledError:
                cancelled.set()
                raise

        async def quick() -> None:
            return None

        clock = asyncio.get_running_loop()
        began = clock.time()
        outcome = await spread_closes(
            [quick, stuck, quick], timedelta(seconds=0.2), tail=timedelta(seconds=0.2)
        )

        assert clock.time() - began <= 0.4 + SLACK
        assert (outcome.closed, outcome.failed, outcome.timed_out) == (2, 0, 1)

        await asyncio.wait_for(cancelled.wait(), timeout=1.0)  # the kill itself, observed

    # ....................... #

    async def test_a_failing_close_is_counted_and_not_raised(self) -> None:
        async def failing() -> None:
            raise RuntimeError("transport already gone")

        async def quick() -> None:
            return None

        outcome = await spread_closes(
            [quick, failing, quick], timedelta(seconds=0.05), tail=timedelta(seconds=0.5)
        )

        assert (outcome.closed, outcome.failed, outcome.timed_out) == (2, 1, 0)

    # ....................... #

    async def test_nothing_to_close_returns_at_once(self) -> None:
        began = time.monotonic()
        outcome = await spread_closes([], timedelta(seconds=10), tail=timedelta(seconds=10))

        assert (outcome.closed, outcome.failed, outcome.timed_out) == (0, 0, 0)
        assert time.monotonic() - began < 0.1

    # ....................... #

    async def test_cancelling_the_call_cancels_the_closes_it_started(self) -> None:
        started = asyncio.Event()
        cancelled = asyncio.Event()

        async def slow() -> None:
            started.set()

            try:
                await asyncio.sleep(60)

            except asyncio.CancelledError:
                cancelled.set()
                raise

        call = asyncio.ensure_future(
            spread_closes([slow, slow], timedelta(seconds=30), tail=timedelta(seconds=1))
        )
        await asyncio.wait_for(started.wait(), timeout=1.0)
        call.cancel()

        with pytest.raises(asyncio.CancelledError):
            await call

        await asyncio.wait_for(cancelled.wait(), timeout=1.0)


# ----------------------- #


class _Recorder:
    """What the step collected and the window and tail it fitted, plus whether it finished."""

    def __init__(self, targets: Sequence[str], *, listing: float = 0.0) -> None:
        self.targets = list(targets)
        self.listing = listing
        self.calls: list[tuple[list[str], float, float]] = []
        self.finished = False

    def collect(self) -> list[str]:
        if self.listing:
            time.sleep(self.listing)  # a listing that costs time, on the loop

        return self.targets

    async def close(self, targets: list[str], window: timedelta, tail: timedelta) -> None:
        self.calls.append((targets, window.total_seconds(), tail.total_seconds()))
        # As long as a close may take: a phase fitted wrongly would be cancelled here.
        await asyncio.sleep(window.total_seconds() + tail.total_seconds() - 0.01)
        self.finished = True


def _step(recorder: _Recorder, *, window: float, tail: float) -> LifecycleStep:
    return spread_drain_lifecycle_step(
        collect=recorder.collect,
        close=recorder.close,
        window=timedelta(seconds=window),
        tail=timedelta(seconds=tail),
        step_id="spread",
    )


async def _drain(step: LifecycleStep, budget: float) -> None:
    runtime = build_runtime(lifecycle_steps=step, drain_timeout=timedelta(0))

    async with runtime.scope():
        await runtime.drain(timedelta(seconds=budget))


class TestSpreadDrainStep:
    async def test_a_small_budget_shrinks_the_window_and_the_tail_and_is_not_overrun(
        self,
    ) -> None:
        recorder = _Recorder(["a", "b"])

        await _drain(_step(recorder, window=10, tail=10), budget=0.6)

        [(targets, window, tail)] = recorder.calls
        assert targets == ["a", "b"]
        # tail = half of what was left after the margin, window = the rest
        assert tail == pytest.approx(0.275, abs=0.02)
        assert window == pytest.approx(tail, abs=0.02)
        assert recorder.finished  # never cancelled mid-close

    # ....................... #

    async def test_a_large_budget_keeps_the_configured_window(self) -> None:
        recorder = _Recorder(["a"])

        await _drain(_step(recorder, window=0.3, tail=0.2), budget=30)

        assert [(window, tail) for _, window, tail in recorder.calls] == [(0.3, 0.2)]

    # ....................... #

    async def test_what_is_left_is_measured_after_listing(self) -> None:
        recorder = _Recorder(["a"], listing=0.3)

        await _drain(_step(recorder, window=10, tail=10), budget=0.8)

        [(_, window, tail)] = recorder.calls
        # (0.8 - 0.05 margin - 0.3 listing) shared: the listing's time is not handed out again
        assert window + tail == pytest.approx(0.45, abs=0.04)
        assert recorder.finished

    # ....................... #

    async def test_below_the_floor_after_listing_the_phase_is_skipped(self) -> None:
        recorder = _Recorder(["a"])

        # 0.12 s clears the runner's floor; less the margin it is under the phase's own.
        await _drain(_step(recorder, window=10, tail=10), budget=0.12)

        assert recorder.calls == []

    # ....................... #

    async def test_nothing_to_close_closes_nothing(self) -> None:
        recorder = _Recorder([])

        await _drain(_step(recorder, window=1, tail=1), budget=5)

        assert recorder.calls == []

    # ....................... #

    @pytest.mark.parametrize(("window", "tail"), [(0, 1), (1, 0), (-1, 1)])
    def test_a_window_or_tail_that_is_not_positive_is_refused(
        self, window: float, tail: float
    ) -> None:
        with pytest.raises(CoreException) as caught:
            _step(_Recorder([]), window=window, tail=tail)

        assert caught.value.kind == ExceptionKind.CONFIGURATION
