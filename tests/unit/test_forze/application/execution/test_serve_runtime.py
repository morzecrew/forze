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

import pytest

from forze.application.contracts.execution import LifecycleStep
from forze.application.execution import ExecutionContext, build_runtime, serve_runtime

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
            _wait_for("startup", lambda: "started" in _events(record), within=30.0)

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
        assert _events(record) == ["started", "phase-started", "phase-done", "teardown", "served"]
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
            "started",
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
        assert _events(record) == ["started", "phase-started", "phase-done", "teardown", "served"]

    # ....................... #

    def test_a_drain_that_raises_still_tears_down(self, tmp_path: Path) -> None:
        with _running(tmp_path, "raise") as (process, record):
            process.send_signal(signal.SIGTERM)
            code = process.wait(timeout=10.0)

        assert code == 0
        assert _events(record)[:2] == ["started", "drain-raised"]
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

        for _ in range(200):  # bounded wait for startup to finish
            if runtime.ready:
                break

            await asyncio.sleep(0.01)

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

            for _ in range(200):  # bounded wait for startup to finish
                if runtime.ready:
                    break

                await asyncio.sleep(0.01)

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
