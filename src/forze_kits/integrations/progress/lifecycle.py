"""Background wiring for the progress plane — the staleness sweep.

One step, and it exists because the staleness question has no natural asker: nothing in the
request path is going to notice that a job stopped reporting four hours ago. This drives
:meth:`JobStalenessMonitor.sweep` on an interval so the gauges
(:func:`~.observability.instrument_job_progress`) always have a recent answer to hand a
scrape.

The sweep is read-only and idempotent, so any interval and any number of replicas are safe;
the jitter only keeps a fleet from asking in lockstep.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Sequence
from datetime import timedelta
from typing import final
from uuid import UUID

import attrs

from forze.application.contracts.execution import LifecycleHook, LifecycleStep
from forze.application.execution import ExecutionContext
from forze.application.execution.background import (
    DEFAULT_STOP_GRACE_SECONDS,
    BackgroundLoopControl,
)
from forze.base.exceptions import CoreException, ExceptionKind, exc
from forze.base.primitives import StrKey, current_entropy_source
from forze_kits.integrations._logger import logger

from .observability import JobStalenessMonitor
from .record import JobDocumentSpec, job_record_spec

# ----------------------- #

DEFAULT_STALENESS_INTERVAL: timedelta = timedelta(minutes=1)
"""How often the sweep asks. Also the resolution of every gauge it feeds."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class _StalenessSweepStartup(LifecycleHook):
    """Run the staleness sweep on an interval until the runtime drains."""

    monitor: JobStalenessMonitor
    interval: timedelta
    jitter: float

    control: BackgroundLoopControl = attrs.field(
        default=attrs.Factory(lambda: BackgroundLoopControl(name="job_staleness")),
        init=False,
    )

    # ....................... #

    @property
    def task(self) -> asyncio.Task[None] | None:
        return self.control.task

    # ....................... #

    @property
    def loop_name(self) -> str:
        """Satisfies ``DrainableLoop``."""

        return self.control.loop_name

    # ....................... #

    async def stop(self, *, deadline: float) -> bool:
        """Stop between ticks. A sweep cut mid-flight costs nothing — it only reads."""

        return await self.control.stop(deadline=deadline)

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        if self.control.running:
            logger.warning("Job staleness sweep already running; ignoring duplicate startup")

            return

        self.control.arm()

        async def _loop() -> None:
            while True:
                try:
                    await self.monitor.sweep(ctx)

                except asyncio.CancelledError:
                    raise

                except CoreException as error:
                    # A misrouted spec or a missing dep does not fix itself on the next
                    # tick, and a loop that keeps failing silently leaves the gauges frozen
                    # at their last value — which reads as "nothing is stuck".
                    if error.kind is ExceptionKind.CONFIGURATION:
                        logger.exception(
                            "Job staleness sweep hit a configuration error; loop stopped — "
                            "fix the wiring and restart (the staleness gauges are now stale, "
                            "which forze.jobs.staleness.scan_age will say)"
                        )

                        return

                    logger.exception("Job staleness sweep failed")

                except Exception:
                    logger.exception("Job staleness sweep failed")

                if await self.control.sleep_or_stop(
                    self.interval.total_seconds()
                    * (
                        1.0
                        + current_entropy_source().as_random().uniform(-self.jitter, self.jitter)
                    )
                ):
                    return

        try:
            self.control.task = asyncio.create_task(_loop(), name=self.control.loop_name)
            ctx.drainables.register(self)

        except BaseException:
            task = self.control.task

            if task is not None:
                task.cancel()

            self.control.task = None
            self.control.event = None
            raise


# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class _StalenessSweepShutdown(LifecycleHook):
    """Stop the sweep loop (fallback for a hand-driven lifecycle; idempotent)."""

    startup: _StalenessSweepStartup

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        clock = asyncio.get_running_loop()
        await self.startup.stop(deadline=clock.time() + DEFAULT_STOP_GRACE_SECONDS)


# ....................... #


def job_staleness_lifecycle_step(
    *,
    silent_after: timedelta,
    spec: JobDocumentSpec | None = None,
    kinds: Sequence[str] = (),
    tenants: Callable[[], Sequence[UUID]] | None = None,
    interval: timedelta = DEFAULT_STALENESS_INTERVAL,
    jitter: float = 0.2,
    step_id: StrKey = "job_staleness",
) -> tuple[LifecycleStep, JobStalenessMonitor]:
    """Sweep for stuck jobs on an interval; returns the step **and** the monitor to instrument.

    Both, because they are two halves of one signal and separating them is how a deployment
    ends up with a sweep nobody reads or a gauge nobody refreshes::

        step, monitor = job_staleness_lifecycle_step(silent_after=timedelta(minutes=15))
        instrument_job_progress(monitor)
        lifecycle_steps.append(step)

    *silent_after* is a decision with no safe default: it must be comfortably longer than
    how often the work in question reports, or every healthy job reads as stuck. *interval*
    is how fresh the answer stays — it, not the scrape interval, is the resolution of the
    gauges. Under a fleet, wrap the step with a singleton if the extra reads matter; the
    sweep is read-only, so concurrent ones are merely redundant.
    """

    if interval.total_seconds() <= 0:
        raise exc.configuration("Job staleness interval must be positive")

    if not 0.0 <= jitter < 1.0:
        raise exc.configuration("Jitter must be in [0, 1)")

    monitor = JobStalenessMonitor(
        silent_after=silent_after,
        spec=spec if spec is not None else job_record_spec(),
        kinds=tuple(kinds),
        tenants=tenants,
    )

    startup = _StalenessSweepStartup(monitor=monitor, interval=interval, jitter=jitter)

    return (
        LifecycleStep(
            id=step_id,
            startup=startup,
            shutdown=_StalenessSweepShutdown(startup=startup),
            requires_long_running=True,
        ),
        monitor,
    )
