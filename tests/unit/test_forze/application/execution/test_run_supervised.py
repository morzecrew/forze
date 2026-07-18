"""Unit tests for the shared crash-restart supervision loop.

# covers: forze.application.execution.background.run_supervised

The supervisor has exactly four exits — stop requested, cancellation, configuration error,
crash ceiling — and one job between them: restart a crashed (or wrongly-returned) loop after
a jittered backoff. Each exit is asserted separately, because the failure mode of a supervisor
is always the same: it takes the wrong exit and either kills a healthy loop or immortalizes a
broken one.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from unittest.mock import MagicMock, patch

import pytest

from forze.application.execution.background import run_supervised
from forze.base.exceptions import CoreException, exc

# ----------------------- #

BACKOFF = timedelta(milliseconds=5)


async def _supervise(run, stop: asyncio.Event, **overrides: object):  # type: ignore[no-untyped-def]
    kwargs: dict[str, object] = {
        "stop": stop,
        "name": "test_loop",
        "restart_backoff": BACKOFF,
    }
    kwargs.update(overrides)

    return await run_supervised(run, **kwargs)  # type: ignore[arg-type]


# ----------------------- #


async def test_restarts_after_crash_until_stop() -> None:
    stop = asyncio.Event()
    runs = 0

    async def _run() -> None:
        nonlocal runs
        runs += 1

        if runs >= 3:
            stop.set()
            return

        raise RuntimeError("boom")

    await asyncio.wait_for(_supervise(_run, stop), timeout=5)

    assert runs == 3  # crashed twice, restarted twice, stopped on the third


# ....................... #


async def test_stop_during_backoff_ends_supervision() -> None:
    stop = asyncio.Event()
    runs = 0

    async def _run() -> None:
        nonlocal runs
        runs += 1
        raise RuntimeError("boom")

    async def _stopper() -> None:
        await asyncio.sleep(0)  # let the first run crash into its backoff
        stop.set()

    task = asyncio.create_task(_supervise(_run, stop, restart_backoff=timedelta(seconds=60)))
    await _stopper()
    await asyncio.wait_for(task, timeout=5)

    assert runs == 1  # the 60s backoff was interrupted by the stop, not slept out


# ....................... #


async def test_clean_return_without_stop_restarts() -> None:
    stop = asyncio.Event()
    runs = 0

    async def _run() -> None:
        nonlocal runs
        runs += 1

        if runs >= 2:
            stop.set()
        # returns cleanly both times; only the second had stop set

    logger_mock = MagicMock()

    with patch("forze.application.execution.background.supervise.logger", logger_mock):
        await asyncio.wait_for(_supervise(_run, stop), timeout=5)

    assert runs == 2
    logger_mock.warning.assert_called_once()  # the unexpected clean return was surfaced


# ....................... #


async def test_configuration_error_is_terminal() -> None:
    stop = asyncio.Event()
    runs = 0

    async def _run() -> None:
        nonlocal runs
        runs += 1
        raise exc.configuration("route is not wired")

    logger_mock = MagicMock()

    with patch("forze.application.execution.background.supervise.logger", logger_mock):
        await asyncio.wait_for(_supervise(_run, stop), timeout=5)

    assert runs == 1  # wiring does not fix itself — no restart
    logger_mock.critical.assert_called_once()


# ....................... #


async def test_cancellation_propagates() -> None:
    stop = asyncio.Event()
    entered = asyncio.Event()

    async def _run() -> None:
        entered.set()
        await asyncio.Event().wait()

    task = asyncio.create_task(_supervise(_run, stop))
    await asyncio.wait_for(entered.wait(), timeout=5)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task


# ....................... #


async def test_crash_ceiling_stops_supervision() -> None:
    stop = asyncio.Event()
    runs = 0

    async def _run() -> None:
        nonlocal runs
        runs += 1
        raise RuntimeError("permanent fault")

    logger_mock = MagicMock()

    with patch("forze.application.execution.background.supervise.logger", logger_mock):
        await asyncio.wait_for(_supervise(_run, stop, max_consecutive_crashes=3), timeout=5)

    assert runs == 3
    logger_mock.critical.assert_called_once()


# ....................... #


async def test_healthy_run_resets_the_crash_streak() -> None:
    stop = asyncio.Event()
    runs = 0

    async def _run() -> None:
        nonlocal runs
        runs += 1

        if runs >= 5:
            stop.set()
            return

        raise RuntimeError("boom")

    # Every run is instantaneous (unhealthy), but with the clock patched to always report a
    # healthy uptime the streak resets each time — so a ceiling of 2 is never reached.
    with patch(
        "forze.application.execution.background.supervise.HEALTHY_UPTIME_SECONDS",
        -1.0,
    ):
        await asyncio.wait_for(_supervise(_run, stop, max_consecutive_crashes=2), timeout=5)

    assert runs == 5  # four crashes, streak reset after each — the ceiling never tripped


# ....................... #


async def test_on_crash_hook_observes_every_crash() -> None:
    stop = asyncio.Event()
    runs = 0
    seen: list[BaseException] = []

    async def _run() -> None:
        nonlocal runs
        runs += 1

        if runs >= 3:
            stop.set()
            return

        raise RuntimeError(f"crash {runs}")

    await asyncio.wait_for(_supervise(_run, stop, on_crash=seen.append), timeout=5)

    assert [str(one) for one in seen] == ["crash 1", "crash 2"]


# ....................... #


async def test_invalid_settings_are_refused() -> None:
    stop = asyncio.Event()

    async def _run() -> None:  # pragma: no cover - never reached
        return

    with pytest.raises(CoreException):
        await _supervise(_run, stop, restart_backoff=timedelta(seconds=0))

    with pytest.raises(CoreException):
        await _supervise(_run, stop, max_consecutive_crashes=0)


async def test_failing_on_crash_observer_does_not_kill_supervision() -> None:
    stop = asyncio.Event()
    runs = 0

    async def _run() -> None:
        nonlocal runs
        runs += 1

        if runs >= 3:
            stop.set()
            return

        raise RuntimeError("boom")

    def _broken_observer(_error: BaseException) -> None:
        raise ValueError("the metrics hook itself is broken")

    logger_mock = MagicMock()

    with patch("forze.application.execution.background.supervise.logger", logger_mock):
        await asyncio.wait_for(_supervise(_run, stop, on_crash=_broken_observer), timeout=5)

    assert runs == 3  # both crashes restarted despite the observer failing every time
    assert logger_mock.error.call_count == 4  # 2 observer failures + 2 crash logs
