"""Close many connections gently: each on its own schedule over a window, never in turn.

A replica leaving with thousands of open connections must not close them all at once — every
client would reconnect to the remaining replicas in the same second — and cannot close them
one after another, awaiting each: a few slow closes push the rest past the grace period.
:func:`spread_closes` starts close *i* of *n* at ``i * window / n`` as its own task, waits a
bounded tail for the stragglers and cancels what is left, so the whole call takes at most
``window + tail``. :func:`spread_drain_lifecycle_step` runs it as a drain phase, fitting the
window and tail to the allowance the phase is given. Both transports' connection drains are
built on them.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Sequence, Sized
from datetime import timedelta
from typing import TYPE_CHECKING, Final, final

import attrs

from forze.application._logger import logger
from forze.application.contracts.execution import LifecycleStep
from forze.base.exceptions import exc
from forze.base.primitives import StrKey

from ..context.lifecycle import DRAIN_PHASE_FLOOR_SECONDS

if TYPE_CHECKING:
    from forze.application.execution.context import ExecutionContext

# ----------------------- #

__all__ = [
    "SpreadOutcome",
    "spread_closes",
    "spread_drain_lifecycle_step",
]

CloseFn = Callable[[], Awaitable[None]]
"""Close one connection. A raise counts as a failed close; it is never propagated."""

_ALLOWANCE_MARGIN_SECONDS: Final[float] = 0.05
"""Kept back from a phase's allowance. The runner's timer starts a loop iteration before the
phase reads its own clock, so a phase that spent its whole allowance would race it and be
cancelled mid-close."""


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class SpreadOutcome:
    """How the closes :func:`spread_closes` started ended."""

    closed: int = 0
    """Finished within the tail."""

    failed: int = 0
    """Raised; each is logged."""

    timed_out: int = 0
    """Still running at the end of the tail, and cancelled."""


# ....................... #


async def _close_one(close: CloseFn) -> bool:
    try:
        await close()

    except Exception:
        logger.warning("A spread close failed", exc_info=True)

        return False

    return True


# ....................... #


async def spread_closes(
    closes: Sequence[CloseFn],
    window: timedelta,
    *,
    tail: timedelta,
) -> SpreadOutcome:
    """Start close *i* of *n* at ``i * window / n``, each as its own task, then wait for them.

    A slow close never delays the next one. After the last start the call waits up to *tail*
    for the started closes, never past ``window + tail`` from its start, then cancels any
    still running and returns, so the whole call takes at most ``window + tail``. A close
    that raises is logged and counted, never propagated.

    :param closes: One callable per connection; deduplicate before calling.
    :param window: Over how long the starts are spread.
    :param tail: How long the started closes may take after the last start.
    :returns: How many closed, failed and timed out.
    """

    if not closes:
        return SpreadOutcome()

    clock = asyncio.get_running_loop()
    started = clock.time()
    window_seconds = max(0.0, window.total_seconds())
    tail_seconds = max(0.0, tail.total_seconds())
    gap = window_seconds / len(closes)
    deadline = started + window_seconds + tail_seconds
    tasks: list[asyncio.Task[bool]] = []

    try:
        for index, close in enumerate(closes):
            delay = started + index * gap - clock.time()

            if delay > 0:
                await asyncio.sleep(delay)

            tasks.append(asyncio.ensure_future(_close_one(close)))

        await asyncio.wait(tasks, timeout=max(0.0, min(tail_seconds, deadline - clock.time())))

    except asyncio.CancelledError:
        # Cancelled from outside: don't leave the started closes running detached.
        for task in tasks:
            task.cancel()

        raise

    closed = failed = timed_out = 0

    for task in tasks:
        if not task.done():
            task.cancel()  # best-effort; a close that ignores it finishes on its own
            timed_out += 1

        elif task.result():
            closed += 1

        else:
            failed += 1

    return SpreadOutcome(closed=closed, failed=failed, timed_out=timed_out)


# ....................... #


def spread_drain_lifecycle_step[T: Sized](
    *,
    collect: Callable[[], T],
    close: Callable[[T, timedelta, timedelta], Awaitable[object]],
    window: timedelta,
    tail: timedelta,
    order: int = 0,
    step_id: StrKey,
) -> LifecycleStep:
    """A lifecycle step whose startup registers a drain phase that closes connections.

    When the scope drains, the phase calls *collect* for what to close, then measures what
    is left of its allowance, ``b``, from a deadline — so the listing's own time counts. The
    tail becomes ``min(tail, b / 2)`` and the window ``min(window, b - that tail)``: a small
    allowance shortens both, a large one never lengthens the configured window. Below the
    drain floor (100 ms) the phase is skipped and the connections close when the server
    stops. Then it awaits ``close(collected, window, tail)``, which must finish within
    ``window + tail`` — :func:`spread_closes` does — so the phase never overruns its
    allowance and is never cancelled mid-close.

    :param collect: Lists what to close, when the phase runs.
    :param close: Closes it over the fitted window and tail.
    :param window: The longest the closes are spread over.
    :param tail: The longest the closes may take after the last start.
    :param order: The phase's place among the scope's drain phases.
    :param step_id: The step's id, which also names its drain phase.

    :raises CoreException: ``configuration`` — *window* or *tail* is not positive.
    """

    if window <= timedelta(0) or tail <= timedelta(0):
        raise exc.configuration(
            f"Drain step {step_id!r} needs a positive window and tail, got {window} and {tail}"
        )

    window_seconds = window.total_seconds()
    tail_seconds = tail.total_seconds()

    async def phase(allowed: timedelta) -> None:
        clock = asyncio.get_running_loop()
        deadline = clock.time() + allowed.total_seconds() - _ALLOWANCE_MARGIN_SECONDS
        collected = collect()

        if not len(collected):
            logger.info("Drain phase '%s': nothing to close", step_id)
            return

        left = deadline - clock.time()

        if left < DRAIN_PHASE_FLOOR_SECONDS:
            logger.warning(
                "Drain phase '%s' skipped: %.2fs left to close %d, under the %.1fs floor; "
                "they close when the server stops",
                step_id,
                max(0.0, left),
                len(collected),
                DRAIN_PHASE_FLOOR_SECONDS,
            )
            return

        fitted_tail = min(tail_seconds, left / 2)
        fitted_window = min(window_seconds, left - fitted_tail)

        logger.info(
            "Drain phase '%s': closing %d over %.2fs, then waiting up to %.2fs",
            step_id,
            len(collected),
            fitted_window,
            fitted_tail,
        )

        await close(
            collected,
            timedelta(seconds=fitted_window),
            timedelta(seconds=fitted_tail),
        )

    async def startup(ctx: ExecutionContext) -> None:
        ctx.drain_phases.register(step_id, phase, order=order)

    return LifecycleStep(id=step_id, startup=startup)
