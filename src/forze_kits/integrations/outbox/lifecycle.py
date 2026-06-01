"""Lifecycle helpers for background outbox relay."""

from __future__ import annotations

import asyncio
from contextlib import suppress
from datetime import timedelta
from typing import Any, final

import attrs

from forze.application._logger import logger
from forze.application.contracts.execution import LifecycleHook, LifecycleStep
from forze.application.contracts.outbox import OutboxSpec
from forze.application.contracts.queue import QueueSpec
from forze.application.execution.context import ExecutionContext
from forze.base.primitives import StrKey

from .relay import relay_outbox_to_queue

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True)
class _OutboxRelayBackgroundStartup(LifecycleHook):
    """Start a background task that periodically relays outbox rows."""

    outbox_spec: OutboxSpec[Any]
    queue_spec: QueueSpec[Any]
    interval: timedelta
    reclaim_stale_after: timedelta | None
    limit: int | None
    task: asyncio.Task[None] | None = attrs.field(default=None, init=False)

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        async def _loop() -> None:
            while True:
                try:
                    await relay_outbox_to_queue(
                        ctx,
                        outbox_spec=self.outbox_spec,
                        queue_spec=self.queue_spec,
                        limit=self.limit,
                        reclaim_stale_after=self.reclaim_stale_after,
                    )
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("Outbox background relay failed")

                await asyncio.sleep(self.interval.total_seconds())

        self.task = asyncio.create_task(_loop(), name="outbox_relay_background")


# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class _OutboxRelayBackgroundShutdown(LifecycleHook):
    """Cancel the background outbox relay task."""

    startup: _OutboxRelayBackgroundStartup
    """Startup hook."""

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:  # noqa: ARG002
        task = self.startup.task

        if task is None:
            return

        task.cancel()

        with suppress(asyncio.CancelledError):
            await task


# ....................... #


def outbox_relay_background_lifecycle_step(
    *,
    outbox_spec: OutboxSpec[Any],
    queue_spec: QueueSpec[Any],
    interval: timedelta = timedelta(seconds=30),
    reclaim_stale_after: timedelta | None = timedelta(minutes=5),
    limit: int | None = None,
    step_id: StrKey = "outbox_relay",
) -> LifecycleStep:
    """Build a lifecycle step that relays outbox rows on a background interval.

    Opt-in for long-running processes. Production deployments often prefer
    external cron or workflow schedulers instead of in-process polling.
    """

    startup = _OutboxRelayBackgroundStartup(
        outbox_spec=outbox_spec,
        queue_spec=queue_spec,
        interval=interval,
        reclaim_stale_after=reclaim_stale_after,
        limit=limit,
    )
    shutdown = _OutboxRelayBackgroundShutdown(startup=startup)

    return LifecycleStep(id=step_id, startup=startup, shutdown=shutdown)
