"""Minimal supervision for the realtime gateway — spawn the run task, cancel it.

Deliberately thin (RFC 0002 §7): it owns the gateway's ``run(ctx)`` task with
structured cancellation and nothing else. The "restart on crash with backoff,
drain on shutdown" concern is **not** baked in here — that belongs to a future
unified background-runner, and keeping supervision separate from the gateway's
work makes adopting it a swap rather than a rewrite. We do not clone the
fused ``queue_consumer_background_lifecycle_step``.
"""

from ._compat import require_socketio

require_socketio()

# ....................... #

import asyncio
from contextlib import suppress
from typing import final

import attrs

from forze.application.contracts.execution import LifecycleHook, LifecycleStep
from forze.application.execution import ExecutionContext
from forze.base.primitives import StrKey

from .gateway import RealtimeGateway

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True)
class _RealtimeGatewayStartup(LifecycleHook):
    """Spawn the gateway's ``run`` loop as a background task."""

    gateway: RealtimeGateway
    task: asyncio.Task[None] | None = attrs.field(default=None, init=False)

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        if self.task is not None and not self.task.done():
            # startup runs once per scope; a duplicate call must not orphan a task
            return

        self.task = asyncio.create_task(self.gateway.run(ctx), name="realtime_gateway")


# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class _RealtimeGatewayShutdown(LifecycleHook):
    """Cancel the gateway's background task with structured cancellation."""

    startup: _RealtimeGatewayStartup

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:  # noqa: ARG002
        task = self.startup.task

        if task is None:
            return

        task.cancel()

        with suppress(asyncio.CancelledError):
            await task


# ----------------------- #


def realtime_gateway_lifecycle_step(
    gateway: RealtimeGateway,
    *,
    step_id: StrKey = "realtime_gateway",
) -> LifecycleStep:
    """Build the minimal lifecycle step that runs *gateway* for the process lifetime."""

    startup = _RealtimeGatewayStartup(gateway=gateway)

    return LifecycleStep(
        id=step_id,
        startup=startup,
        shutdown=_RealtimeGatewayShutdown(startup=startup),
        requires_long_running=True,
    )
