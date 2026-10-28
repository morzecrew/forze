"""``serve_runtime`` routes SIGTERM into announce → drain phases → teardown, in a real process."""

from __future__ import annotations

import asyncio
import signal
import subprocess
import sys
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import timedelta
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from forze.application.contracts.execution import LifecycleStep
from forze.application.execution import ExecutionContext, build_runtime, serve_runtime
from forze.application.execution.lifecycle.serving import SignalDrain
from forze.application.execution.runtime import ExecutionRuntime

# ----------------------- #

_APP = Path(__file__).with_name("_serve_runtime_app.py")

ANNOUNCE_DELAY = 0.5


def _events(record: Path) -> list[str]:
    return record.read_text().split() if record.exists() else []


def _wait_for(condition: str, check: Callable[[], bool], *, within: float) -> None:
    deadline = time.monotonic() + within

    while time.monotonic() < deadline:
        if check():
            return

        time.sleep(0.02)

    raise AssertionError(f"timed out after {within}s waiting for {condition}")


async def _until_ready(runtime: ExecutionRuntime) -> None:
    for _ in range(200):  # bounded wait for startup to finish
        if runtime.ready:
            return

        await asyncio.sleep(0.01)


@contextmanager
def _running(tmp_path: Path, mode: str) -> Iterator[tuple[subprocess.Popen[bytes], Path]]:
    record = tmp_path / "events"

    with (tmp_path / "stderr").open("wb") as stderr:
        # ``-P``: this directory's test packages must not shadow anything the app imports.
        process = subprocess.Popen(
            [sys.executable, "-P", str(_APP), str(record), mode],
            stdout=subprocess.DEVNULL,
            stderr=stderr,
        )

        try:
            _wait_for("readiness", lambda: "ready" in _events(record), within=30.0)

            yield process, record

        except BaseException:
            sys.stderr.write((tmp_path / "stderr").read_text())
            raise

        finally:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=10)


# ----------------------- #


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX signals")
class TestServeRuntime:
    def test_sigterm_drains_then_tears_down_and_returns(self, tmp_path: Path) -> None:
        with _running(tmp_path, "quick") as (process, record):
            started = time.monotonic()
            process.send_signal(signal.SIGTERM)
            code = process.wait(timeout=10.0)
            elapsed = time.monotonic() - started

        assert code == 0
        assert _events(record) == ["ready", "phase-started", "phase-done", "teardown", "served"]
        # The phase waited for the announcement: the drain ran, not shutdown's own.
        assert elapsed >= ANNOUNCE_DELAY

    # ....................... #

    def test_a_second_sigint_cancels_a_slow_phase_and_tears_down_at_once(
        self,
        tmp_path: Path,
    ) -> None:
        with _running(tmp_path, "slow") as (process, record):
            process.send_signal(signal.SIGINT)
            _wait_for(
                "the slow phase to start",
                lambda: "phase-started" in _events(record),
                within=ANNOUNCE_DELAY + 5.0,
            )

            started = time.monotonic()
            process.send_signal(signal.SIGINT)
            code = process.wait(timeout=10.0)
            elapsed = time.monotonic() - started

        assert code == 0
        assert _events(record) == [
            "ready",
            "phase-started",
            "phase-cancelled",
            "teardown",
            "served",
        ]
        assert elapsed < 3.0

    # ....................... #

    def test_a_repeated_sigterm_does_not_cut_the_drain(self, tmp_path: Path) -> None:
        with _running(tmp_path, "timed") as (process, record):
            process.send_signal(signal.SIGTERM)
            _wait_for(
                "the phase to start",
                lambda: "phase-started" in _events(record),
                within=ANNOUNCE_DELAY + 5.0,
            )

            process.send_signal(signal.SIGTERM)
            code = process.wait(timeout=10.0)

        assert code == 0
        assert _events(record) == ["ready", "phase-started", "phase-done", "teardown", "served"]

    # ....................... #

    def test_a_drain_that_raises_still_tears_down(self, tmp_path: Path) -> None:
        with _running(tmp_path, "raise") as (process, record):
            process.send_signal(signal.SIGTERM)
            code = process.wait(timeout=10.0)

        assert code == 0
        assert _events(record)[:2] == ["ready", "drain-raised"]
        assert _events(record)[-2:] == ["teardown", "served"]


# ----------------------- #


class TestStopAndHandlers:
    @pytest.mark.asyncio
    async def test_the_handlers_are_installed_while_serving_and_restored_after(self) -> None:
        handled = (signal.SIGINT, signal.SIGTERM)
        before = {sig: signal.getsignal(sig) for sig in handled}
        runtime = build_runtime(drain_timeout=timedelta(0))
        stop = asyncio.Event()
        serving = asyncio.create_task(
            serve_runtime(runtime, drain_budget=timedelta(seconds=1), stop=stop)
        )

        await _until_ready(runtime)
        during = {sig: signal.getsignal(sig) for sig in handled}
        stop.set()
        await asyncio.wait_for(serving, timeout=10.0)

        assert all(during[sig] != before[sig] for sig in handled)
        assert {sig: signal.getsignal(sig) for sig in handled} == before

    # ....................... #

    def test_off_the_main_thread_the_stop_event_drains_and_tears_down(self) -> None:
        """No signal handler can be installed there; the event is the way in."""

        events: list[str] = []
        before = signal.getsignal(signal.SIGTERM)

        async def phase(_allowed: timedelta) -> None:
            events.append("phase")

        async def startup(ctx: ExecutionContext) -> None:
            ctx.drain_phases.register("phase", phase)
            events.append(f"handler-installed={signal.getsignal(signal.SIGTERM) != before}")

        async def teardown(_ctx: ExecutionContext) -> None:
            events.append("teardown")

        runtime = build_runtime(
            lifecycle_steps=LifecycleStep(id="step", startup=startup, shutdown=teardown),
            drain_timeout=timedelta(0),
        )

        async def worker() -> None:
            stop = asyncio.Event()
            serving = asyncio.create_task(
                serve_runtime(runtime, drain_budget=timedelta(seconds=1), stop=stop)
            )

            await _until_ready(runtime)
            assert runtime.ready
            stop.set()

            await asyncio.wait_for(serving, timeout=10.0)
            events.append("served")

        failures: list[BaseException] = []

        def run() -> None:
            try:
                asyncio.run(worker())

            except BaseException as error:  # surfaced to the test thread below
                failures.append(error)

        thread = threading.Thread(target=run)
        thread.start()
        thread.join(timeout=20.0)

        assert not thread.is_alive()
        assert failures == []
        assert events == ["handler-installed=False", "phase", "teardown", "served"]

    # ....................... #

    @pytest.mark.asyncio
    async def test_cancelling_it_mid_drain_cancels_the_drain_and_tears_down_at_once(
        self,
    ) -> None:
        handled = (signal.SIGINT, signal.SIGTERM)
        before = {sig: signal.getsignal(sig) for sig in handled}
        events: list[str] = []
        phase_started = asyncio.Event()

        async def phase(_allowed: timedelta) -> None:
            events.append("phase-started")
            phase_started.set()

            try:
                await asyncio.sleep(60)

            except asyncio.CancelledError:
                events.append("phase-cancelled")
                raise

        async def startup(ctx: ExecutionContext) -> None:
            ctx.drain_phases.register("slow", phase)

        async def teardown(_ctx: ExecutionContext) -> None:
            events.append("teardown")

        runtime = build_runtime(
            lifecycle_steps=LifecycleStep(id="step", startup=startup, shutdown=teardown),
            drain_timeout=timedelta(0),
        )
        stop = asyncio.Event()
        serving = asyncio.create_task(
            serve_runtime(runtime, drain_budget=timedelta(seconds=30), stop=stop)
        )

        await _until_ready(runtime)
        stop.set()
        await asyncio.wait_for(phase_started.wait(), timeout=5.0)

        started = time.monotonic()
        serving.cancel()
        await asyncio.wait({serving}, timeout=10.0)

        # Not after the 30 s budget: the scope's shutdown did not wait the drain out.
        assert serving.cancelled()
        assert time.monotonic() - started < 3.0
        assert events == ["phase-started", "phase-cancelled", "teardown"]
        assert {sig: signal.getsignal(sig) for sig in handled} == before

    # ....................... #

    @pytest.mark.asyncio
    async def test_cancelling_it_during_the_announcement_gives_the_phases_up(self) -> None:
        events: list[str] = []

        async def phase(_allowed: timedelta) -> None:
            events.append("phase")

        async def startup(ctx: ExecutionContext) -> None:
            ctx.drain_phases.register("quick", phase)

        async def teardown(_ctx: ExecutionContext) -> None:
            events.append("teardown")

        runtime = build_runtime(
            lifecycle_steps=LifecycleStep(id="step", startup=startup, shutdown=teardown),
            drain_timeout=timedelta(0),
        )
        stop = asyncio.Event()
        serving = asyncio.create_task(
            serve_runtime(
                runtime,
                drain_budget=timedelta(seconds=30),
                announce_delay=timedelta(seconds=30),
                stop=stop,
            )
        )

        await _until_ready(runtime)
        stop.set()

        for _ in range(500):  # bounded: the announcement starts within a few loop turns
            if runtime.draining:
                break

            await asyncio.sleep(0.01)

        assert runtime.draining

        serving.cancel()
        await asyncio.wait({serving}, timeout=10.0)

        # The drain gave its claim back when cancelled before its phases started; the
        # teardown right after must not take it up and run them.
        assert serving.cancelled()
        assert events == ["teardown"]


# ----------------------- #


def _signal_drain() -> SignalDrain:
    return SignalDrain(
        build_runtime(),
        drain_budget=timedelta(seconds=1),
        announce_delay=timedelta(0),
    )


class TestSignalDrain:
    @pytest.mark.asyncio
    async def test_a_previous_handler_installed_from_c_gives_way_to_the_default(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Python reports such a handler as ``None``, and ``signal.signal`` refuses ``None``."""

        handled = (signal.SIGINT, signal.SIGTERM)
        before = {sig: signal.getsignal(sig) for sig in handled}
        install = signal.signal

        def installed_from_c(sig: int, handler: Any) -> None:
            install(sig, handler)

        monkeypatch.setattr(signal, "signal", installed_from_c)
        drain = _signal_drain()

        try:
            with drain.capture_signals():
                pass

            assert [signal.getsignal(sig) for sig in handled] == [signal.SIG_DFL] * 2

        finally:
            for sig, handler in before.items():
                install(sig, handler)

    # ....................... #

    @pytest.mark.asyncio
    @pytest.mark.parametrize("first", [signal.SIGTERM, signal.SIGINT])
    async def test_a_sigint_in_the_same_loop_turn_still_ends_the_drain(self, first: int) -> None:
        """The second signal cancels the drain before its first step, so its body never runs."""

        drain = _signal_drain()

        drain._on_signal_in_loop(first)  # pyright: ignore[reportPrivateUsage]
        drain._on_signal_in_loop(signal.SIGINT)  # pyright: ignore[reportPrivateUsage]

        await asyncio.wait_for(drain.drained.wait(), timeout=5.0)

    # ....................... #

    @pytest.mark.asyncio
    @pytest.mark.parametrize("again", [signal.SIGTERM, signal.SIGINT])
    async def test_a_signal_after_the_drain_reports_the_teardown(
        self,
        monkeypatch: pytest.MonkeyPatch,
        again: int,
    ) -> None:
        log = MagicMock()
        monkeypatch.setattr(SignalDrain, "log", log)
        drain = _signal_drain()

        with drain.capture_signals():
            drain._on_signal(signal.SIGTERM, None)  # pyright: ignore[reportPrivateUsage]
            await asyncio.wait_for(drain.drained.wait(), timeout=5.0)
            drain._on_signal(again, None)  # pyright: ignore[reportPrivateUsage]

            for _ in range(100):  # bounded: the callback runs within a few loop turns
                if log.warning.called:
                    break

                await asyncio.sleep(0.01)

        log.warning.assert_called_once_with(
            "%s received after the drain: teardown in progress",
            signal.Signals(again).name,
        )
