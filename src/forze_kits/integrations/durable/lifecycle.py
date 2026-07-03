"""Lifecycle helper: a background scanner that recovers abandoned durable runs."""

from __future__ import annotations

import asyncio
from contextlib import suppress
from datetime import timedelta
from typing import final

import attrs

from forze.application.contracts.execution import LifecycleHook, LifecycleStep
from forze.application.execution.context import ExecutionContext
from forze.base.exceptions import exc
from forze.base.primitives import StrKey, current_entropy_source

from forze_kits.integrations._logger import logger

from .runner import DurableFunctionRunner

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True)
class _DurableRecoveryBackgroundStartup(LifecycleHook):
    """Start a background task that periodically recovers abandoned durable runs."""

    runner: DurableFunctionRunner
    """The runner whose registry re-invokes each reclaimed run."""

    interval: timedelta
    """Delay between recovery sweeps."""

    jitter: float
    """Multiplicative sleep jitter in ``[0, 1)`` (desynchronizes N replicas' scanners)."""

    limit: int
    """Runs claimed per sweep batch (also the drain-detection threshold)."""

    max_batches_per_tick: int
    """Safety cap on batches per sweep so a large backlog cannot starve the loop."""

    max_concurrency: int | None
    """Recover a batch's runs concurrently up to this bound (``None`` = sequential)."""

    task: asyncio.Task[None] | None = attrs.field(default=None, init=False)

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.interval.total_seconds() <= 0:
            raise exc.configuration("Interval must be positive")

        if not 0.0 <= self.jitter < 1.0:
            raise exc.configuration("Jitter must be in [0, 1)")

        if self.limit < 1:
            raise exc.configuration("Limit must be >= 1")

        if self.max_batches_per_tick < 1:
            raise exc.configuration("Max batches per tick must be >= 1")

    # ....................... #

    async def _recover_tick(self, ctx: ExecutionContext) -> None:
        """Drain abandoned runs: recover batches until a sweep comes back short."""

        for _ in range(self.max_batches_per_tick):
            claimed = await self.runner.recover(
                ctx, limit=self.limit, max_concurrency=self.max_concurrency
            )

            if claimed < self.limit:
                break

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        async def _loop() -> None:
            while True:
                try:
                    await self._recover_tick(ctx)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("Durable recovery sweep failed")

                # Multiplicative jitter desynchronizes N replicas' scanners so they don't
                # synchronize into a thundering herd against the claim query.
                await asyncio.sleep(
                    self.interval.total_seconds()
                    * (
                        1.0
                        + current_entropy_source()
                        .as_random()
                        .uniform(-self.jitter, self.jitter)
                    )
                )

        if self.task is not None and not self.task.done():
            logger.warning(
                "Durable recovery already running; ignoring duplicate startup"
            )
            return

        self.task = asyncio.create_task(_loop(), name="durable_recovery_background")


# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class _DurableRecoveryBackgroundShutdown(LifecycleHook):
    """Cancel the background durable-recovery task."""

    startup: _DurableRecoveryBackgroundStartup

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:  # noqa: ARG002
        task = self.startup.task

        if task is None:
            return

        task.cancel()

        with suppress(asyncio.CancelledError):
            await task


# ....................... #


def durable_recovery_background_lifecycle_step(
    *,
    runner: DurableFunctionRunner,
    interval: timedelta = timedelta(seconds=30),
    jitter: float = 0.2,
    limit: int = 10,
    max_batches_per_tick: int = 100,
    max_concurrency: int | None = None,
    step_id: StrKey = "durable_recovery",
) -> LifecycleStep:
    """Build a lifecycle step that recovers abandoned durable runs on a background interval.

    Each sweep **drains the backlog**: batches of up to *limit* abandoned runs (a due
    ``PENDING`` run or a ``RUNNING`` run past its lease) are re-claimed and re-invoked until
    a sweep returns fewer than *limit*, capped at *max_batches_per_tick*; then the task
    sleeps *interval* with multiplicative *jitter*. A run's completed steps replay from the
    journal, so each step effect applies exactly once across the crash. *max_concurrency*
    bounds how many runs a batch recovers at once (``None`` = sequential).

    Concurrent scanners are safe (``FOR UPDATE SKIP LOCKED`` + a fence on the terminal
    write), so this can run on every replica; pair with the ``forze_kits`` singleton
    lifecycle guard if you prefer a single elected scanner. Production deployments often
    prefer an external cron / workflow scheduler over in-process polling.
    """

    startup = _DurableRecoveryBackgroundStartup(
        runner=runner,
        interval=interval,
        jitter=jitter,
        limit=limit,
        max_batches_per_tick=max_batches_per_tick,
        max_concurrency=max_concurrency,
    )
    shutdown = _DurableRecoveryBackgroundShutdown(startup=startup)

    return LifecycleStep(id=step_id, startup=startup, shutdown=shutdown)
