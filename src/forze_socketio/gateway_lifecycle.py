"""Supervised lifecycle for the realtime gateway.

The gateway runs under the shared core runner: restart on crash with jittered backoff
(:func:`~forze.application.execution.background.run_supervised`), graceful stop at a batch
boundary (:class:`~forze.application.execution.background.BackgroundLoopControl`), and
registration in ``ctx.drainables`` so ``runtime.shutdown()`` asks it to finish its in-flight
batch **before** lifecycle teardown — the same operational bar as every kits background loop.

A configuration error is terminal (wiring does not fix itself; the supervisor logs it as
critical and stops); everything else restarts. The step's own shutdown hook is the fallback
for a hand-driven lifecycle — the runtime normally stops the loop via the drainables registry
first, and ``stop`` is idempotent.
"""

from ._compat import require_socketio

require_socketio()

# ....................... #

import asyncio
from datetime import timedelta
from typing import final

import attrs

from forze.application.contracts.execution import LifecycleHook, LifecycleStep
from forze.application.execution import ExecutionContext
from forze.application.execution.background import (
    DEFAULT_STOP_GRACE_SECONDS,
    BackgroundLoopControl,
    run_supervised,
)
from forze.base.logging import Logger
from forze.base.primitives import StrKey

from ._logging import ForzeSocketIOLogger
from .gateway import RealtimeGateway

# ----------------------- #

_logger = Logger(ForzeSocketIOLogger.ERRORS)


@final
@attrs.define(slots=True, kw_only=True)
class _RealtimeGatewayStartup(LifecycleHook):
    """Spawn the gateway's supervised ``run`` loop as a background task."""

    gateway: RealtimeGateway
    restart_backoff: timedelta
    max_consecutive_crashes: int | None

    control: BackgroundLoopControl = attrs.field(
        default=attrs.Factory(lambda: BackgroundLoopControl(name="realtime_gateway")),
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
        """Stop the gateway at its next batch boundary. Idempotent.

        Cancelling the gateway mid-batch loses in-flight ephemeral frames and leaves a
        durable signal to the (delayed) reclaim path; stopping between batches costs one
        read cycle.
        """

        return await self.control.stop(deadline=deadline)

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        if self.control.running:
            # The runtime invokes startup once per scope; a direct double call must not
            # leak (and orphan) the previous gateway task.
            _logger.critical("Realtime gateway already running; ignoring duplicate startup")
            return

        stop = self.control.arm()

        async def _run_once() -> None:
            await self.gateway.run(ctx, stop=stop)

        self.control.task = asyncio.create_task(
            run_supervised(
                _run_once,
                stop=stop,
                name=self.control.loop_name,
                restart_backoff=self.restart_backoff,
                max_consecutive_crashes=self.max_consecutive_crashes,
            ),
            name=self.control.loop_name,
        )
        ctx.drainables.register(self)


# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class _RealtimeGatewayShutdown(LifecycleHook):
    """Stop the gateway loop.

    Normally a no-op — the runtime stops every registered loop before teardown begins. This
    is the fallback for a hand-driven lifecycle; ``stop`` is idempotent.
    """

    startup: _RealtimeGatewayStartup

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        clock = asyncio.get_running_loop()
        await self.startup.stop(deadline=clock.time() + DEFAULT_STOP_GRACE_SECONDS)


# ----------------------- #


def realtime_gateway_lifecycle_step(
    gateway: RealtimeGateway,
    *,
    restart_backoff: timedelta = timedelta(seconds=5),
    max_consecutive_crashes: int | None = None,
    step_id: StrKey = "realtime_gateway",
) -> LifecycleStep:
    """Build the supervised lifecycle step that runs *gateway* for the process lifetime.

    :param restart_backoff: Base backoff between restarts after a crash (jittered ×[1.0, 1.5)).
    :param max_consecutive_crashes: Optional terminal ceiling on short-lived runs; ``None``
        restarts forever (every crash is still logged loudly). A configuration error is
        always terminal regardless.
    """

    startup = _RealtimeGatewayStartup(
        gateway=gateway,
        restart_backoff=restart_backoff,
        max_consecutive_crashes=max_consecutive_crashes,
    )

    return LifecycleStep(
        id=step_id,
        startup=startup,
        shutdown=_RealtimeGatewayShutdown(startup=startup),
        requires_long_running=True,
    )
