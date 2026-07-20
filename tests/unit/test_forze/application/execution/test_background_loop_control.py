"""Unit tests for the shared stop machinery behind every background loop.

# covers: forze.application.execution.background.BackgroundLoopControl

The contract is two-sided and both sides matter: a loop that reaches its unit boundary in time
stops *on its own* and says so, and one that does not is cancelled — loudly, and reported as the
failure it is. Getting the second half silently wrong is worse than not having it: every caller
would be told the loop finished its unit of work when it had been killed part-way through.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from unittest.mock import MagicMock, patch

import pytest

from forze.application.execution.background import BackgroundLoopControl
from forze.base.exceptions import CoreException

# ----------------------- #


def _control(**overrides: object) -> BackgroundLoopControl:
    kwargs: dict[str, object] = {"name": "test_loop", "stop_grace": timedelta(seconds=5)}
    kwargs.update(overrides)

    return BackgroundLoopControl(**kwargs)  # type: ignore[arg-type]


def _now() -> float:
    return asyncio.get_running_loop().time()


# ....................... #


def test_non_positive_stop_grace_is_refused() -> None:
    with pytest.raises(CoreException):
        _control(stop_grace=timedelta(seconds=0))


# ....................... #


@pytest.mark.asyncio
async def test_a_loop_that_reaches_its_boundary_stops_on_its_own() -> None:
    control = _control()
    control.arm()
    ticks = 0

    async def loop() -> None:
        nonlocal ticks

        while True:
            ticks += 1

            if await control.sleep_or_stop(0.01):
                return

    control.task = asyncio.create_task(loop())
    await asyncio.sleep(0.05)

    graceful = await control.stop(deadline=_now() + 1.0)

    assert graceful is True
    assert ticks > 0
    assert control.task is not None and control.task.done()
    assert not control.task.cancelled()  # it returned; it was not killed


# ....................... #


@pytest.mark.asyncio
async def test_a_loop_that_overruns_is_cancelled_and_reported_as_cancelled() -> None:
    """The regression: ``stop()`` used to report a graceful stop for a loop it had killed.

    ``asyncio.wait_for`` **cancels what it waits on** when it times out. Waiting on the task
    directly therefore killed the loop mid-work inside the wait itself — after which
    ``task.done()`` is true, so ``stop()`` returned ``True`` ("reached a unit boundary and
    stopped on its own"), the "cancelling it mid-work" warning never fired, and the explicit
    cancel below it was unreachable. Callers were told the loop had finished its unit of work
    when it had been cut in half, and nothing was logged. Only the shield makes the two
    outcomes distinguishable.
    """

    control = _control(stop_grace=timedelta(seconds=5))
    control.arm()
    started = asyncio.Event()

    async def loop() -> None:
        started.set()
        await asyncio.sleep(30)  # never looks at the stop event — cannot reach a boundary

    control.task = asyncio.create_task(loop())
    await asyncio.wait_for(started.wait(), timeout=1.0)

    logger_mock = MagicMock()

    with patch("forze.application.execution.background.loop.logger", logger_mock):
        graceful = await control.stop(deadline=_now() + 0.05)

    assert graceful is False, "a loop killed mid-work must not be reported as a graceful stop"
    logger_mock.warning.assert_called_once()
    assert control.task is not None and control.task.cancelled()


# ....................... #


@pytest.mark.asyncio
async def test_stop_is_idempotent_and_never_leaks_cancelled_error() -> None:
    """A second stop runs from a lifecycle hook after the runtime already stopped the loop.

    It must not raise: a ``CancelledError`` escaping a shutdown hook aborts the teardown of
    every remaining step, the database pool included.
    """

    control = _control()
    control.arm()

    async def loop() -> None:
        await asyncio.sleep(30)

    control.task = asyncio.create_task(loop())

    first = await control.stop(deadline=_now() + 0.05)
    second = await control.stop(deadline=_now() + 0.05)  # already stopped — a no-op

    assert first is False  # it was cancelled
    assert second is True  # nothing left to stop


# ....................... #


@pytest.mark.asyncio
async def test_a_fresh_event_is_armed_per_startup() -> None:
    """A reused event still carries the previous shutdown's stop, and the next loop would exit
    before its first tick without ever saying so."""

    control = _control()
    control.arm()
    control.request_stop()

    assert control.stopping is True

    control.arm()  # the next startup

    assert control.stopping is False
