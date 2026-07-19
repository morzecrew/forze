"""A supervised periodic tick as a lifecycle step — shared by every edge package.

The pattern every connection-hygiene loop repeats (presence heartbeat, expiry
sweep, SSE presence refresh): run *tick* every *interval* on the shared
background-loop machinery — one bad tick can't kill the loop, a stop request is
honored between ticks (interruptible sleep, so shutdown never waits out an
interval), and the loop registers in ``ctx.drainables`` so the runtime stops it
cleanly before teardown. Ticks should be short and idempotent; anything heavier
belongs on :func:`~forze.application.execution.background.run_supervised` with
its restart/backoff semantics instead.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import TYPE_CHECKING, final

import attrs

from forze.application._logger import logger
from forze.application.contracts.execution import LifecycleHook, LifecycleStep
from forze.base.exceptions import exc
from forze.base.primitives import StrKey

from .loop import DEFAULT_STOP_GRACE_SECONDS, BackgroundLoopControl

if TYPE_CHECKING:
    from forze.application.execution.context import ExecutionContext

# ----------------------- #

__all__ = [
    "periodic_lifecycle_step",
]


@final
@attrs.define(slots=True, kw_only=True)
class _PeriodicStartup(LifecycleHook):
    """Run *tick* every *interval* until stopped; one bad tick can't kill the loop."""

    tick: Callable[[], Awaitable[None]]
    interval: timedelta
    name: str

    control: BackgroundLoopControl = attrs.field(
        default=attrs.Factory(
            lambda self: BackgroundLoopControl(name=self.name),
            takes_self=True,
        ),
        init=False,
    )
    """Stop signal and bounded teardown, shared with every other background loop."""

    # ....................... #

    @property
    def task(self) -> asyncio.Task[None] | None:
        """The running loop, if any."""

        return self.control.task

    # ....................... #

    @property
    def loop_name(self) -> str:
        """Satisfies ``DrainableLoop``."""

        return self.control.loop_name

    # ....................... #

    async def stop(self, *, deadline: float) -> bool:
        """Stop the loop between ticks. Idempotent — a tick is short and idempotent too."""

        return await self.control.stop(deadline=deadline)

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        if self.control.running:
            return

        self.control.arm()
        self.control.task = asyncio.create_task(self._loop(), name=self.control.loop_name)
        ctx.drainables.register(self)

    # ....................... #

    async def _loop(self) -> None:
        delay = self.interval.total_seconds()

        while True:
            try:
                await self.tick()

            except asyncio.CancelledError:
                raise

            except Exception:
                logger.critical("Periodic loop %s tick failed", self.name, exc_info=True)

            if await self.control.sleep_or_stop(delay):
                return


# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class _PeriodicShutdown(LifecycleHook):
    """Stop the periodic loop.

    Normally a no-op — the runtime stops every registered loop before teardown begins. This
    is the fallback for a hand-driven lifecycle; ``stop`` is idempotent.
    """

    startup: _PeriodicStartup

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        clock = asyncio.get_running_loop()
        await self.startup.stop(deadline=clock.time() + DEFAULT_STOP_GRACE_SECONDS)


# ----------------------- #


def periodic_lifecycle_step(
    *,
    tick: Callable[[], Awaitable[None]],
    interval: timedelta,
    name: str,
    step_id: StrKey,
) -> LifecycleStep:
    """Build a supervised lifecycle step running *tick* every *interval*.

    :param name: The background loop's name (shows in task names, drainables, and logs).
    """

    if interval.total_seconds() <= 0:
        # A non-positive interval would spin the worker (sleep(0) hot-loop) — fail at wiring.
        raise exc.configuration(f"{name} interval must be positive")

    startup = _PeriodicStartup(tick=tick, interval=interval, name=name)

    return LifecycleStep(
        id=step_id,
        startup=startup,
        shutdown=_PeriodicShutdown(startup=startup),
        requires_long_running=True,
    )
