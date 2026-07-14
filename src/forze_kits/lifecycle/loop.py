"""Stop signal and bounded teardown for a background poll/consume loop.

Every background step in kits owns one of these. It is what turns *"cancel the task"* into
*"ask the loop to stop between units of work, then wait for it"* — the difference between a
consumer that acks the message it was handling and one that leaves it to be redelivered.

The loop still decides *where* its unit boundaries are (after a tick, after an ack, after a
commit); this only supplies the signal, the interruptible sleep, and the bounded stop.
"""

from __future__ import annotations

import asyncio
from contextlib import suppress
from datetime import timedelta
from typing import final

import attrs

from forze.base.exceptions import exc
from forze_kits.lifecycle._logger import logger

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True)
class BackgroundLoopControl:
    """The stop machinery for one background loop."""

    name: str
    """Identifies the loop in shutdown logs — ``queue_consumer:jobs``."""

    stop_grace: timedelta = timedelta(seconds=5)
    """How long the loop gets to reach its next unit boundary before it is cancelled outright.

    Cancelling is the backstop, not the plan: it costs whatever the loop was mid-way through.
    Bound by the caller's deadline as well, so this never extends the runtime's shutdown budget.
    """

    task: asyncio.Task[None] | None = attrs.field(default=None, init=False)
    """The running loop, or ``None`` before its first startup."""

    event: asyncio.Event | None = attrs.field(default=None, init=False, repr=False)
    """The stop signal. Created **fresh at every startup** — an event reused across scopes would
    still carry the previous shutdown's stop, and the next loop would exit before its first tick
    without ever saying so."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.stop_grace.total_seconds() <= 0:
            raise exc.configuration("Stop grace must be positive")

    # ....................... #

    @property
    def loop_name(self) -> str:
        """Satisfies ``DrainableLoop`` for a hook that delegates to this control."""

        return self.name

    # ....................... #

    @property
    def running(self) -> bool:
        """Whether a loop task is currently alive."""

        return self.task is not None and not self.task.done()

    # ....................... #

    @property
    def stopping(self) -> bool:
        """Whether a stop has been requested. Loops check this at their unit boundaries."""

        return self.event is not None and self.event.is_set()

    # ....................... #

    def arm(self) -> None:
        """Arm a fresh stop signal. Call at startup, *after* the duplicate-startup guard."""

        self.event = asyncio.Event()

    # ....................... #

    def request_stop(self) -> None:
        """Ask the loop to stop at its next unit boundary, and wake it from any tick sleep."""

        if self.event is not None:
            self.event.set()

    # ....................... #

    async def sleep_or_stop(self, delay: float) -> bool:
        """Wait out a tick interval; ``True`` when a stop was requested.

        Interruptible, so shutdown neither sits through a whole interval nor has to cancel the
        loop mid-tick to end it.
        """

        event = self.event

        if event is None:  # pragma: no cover - a loop always runs with an armed event
            await asyncio.sleep(delay)
            return False

        with suppress(TimeoutError):
            await asyncio.wait_for(event.wait(), timeout=delay)

        return event.is_set()

    # ....................... #

    async def stop(self, *, deadline: float) -> bool:
        """Ask the loop to stop, wait for it, and cancel it if it overruns.

        Idempotent: the runtime stops loops before teardown and the step's own shutdown hook
        may ask again, so a second call on an already-stopped loop is a no-op. Returning before
        any ``await task`` is also what keeps that second call from raising ``CancelledError``
        out of a lifecycle hook — which would abort teardown of every remaining step.

        :returns: ``True`` when the loop reached a unit boundary and stopped on its own.
        """

        task = self.task

        if task is None or task.done():
            return True

        self.request_stop()

        clock = asyncio.get_running_loop()
        budget = min(self.stop_grace.total_seconds(), max(0.0, deadline - clock.time()))

        # Shielded, because ``wait_for`` **cancels what it is waiting on** when it times out.
        # Waiting on the task directly would therefore kill the loop mid-work right here — and
        # then ``task.done()`` is true, so this reports that it stopped on its own, the warning
        # below never fires, and the explicit cancel is dead code. The loop would be killed
        # exactly as bluntly as before, silently, while claiming a graceful stop.
        with suppress(TimeoutError):
            await asyncio.wait_for(asyncio.shield(task), timeout=budget)

        if task.done():
            return True

        logger.warning(
            "Background loop %s did not reach a stopping point within %.1fs; cancelling it "
            "mid-work",
            self.name,
            budget,
        )

        task.cancel()

        with suppress(asyncio.CancelledError):
            await task

        return False
