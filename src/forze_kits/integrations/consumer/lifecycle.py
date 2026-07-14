"""Lifecycle helpers for the background queue consumer."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import Any, Final, final

import attrs

from forze.application.contracts.execution import LifecycleHook, LifecycleStep
from forze.application.contracts.inbox import InboxSpec
from forze.application.contracts.queue import QueueMessage, QueueSpec
from forze.application.execution.context import ExecutionContext
from forze.base.exceptions import exc
from forze.base.primitives import StrKey, current_entropy_source
from forze_kits.integrations._logger import logger
from forze_kits.lifecycle import BackgroundLoopControl

from .runner import QueueConsumer

# ----------------------- #

_STOP_GRACE_SECONDS: Final[float] = 5.0
"""Fallback budget when a hook stops a loop directly (the runtime supplies its own)."""


@final
@attrs.define(slots=True, kw_only=True)
class _QueueConsumerBackgroundStartup(LifecycleHook):
    """Start a background task that runs a :class:`QueueConsumer` forever.

    The consumer is built by *consumer_factory* at startup (with the scope's
    context in hand), so a handler that resolves ports off the context can be
    closed over it; a pre-built consumer rides a constant factory.
    """

    queue: str
    """Logical queue name, for task naming and crash logs."""

    consumer_factory: Callable[[ExecutionContext], QueueConsumer[Any]]
    """Builds the configured consumer once at startup, from the scope's context."""

    restart_backoff: timedelta

    # ....................... #

    control: BackgroundLoopControl = attrs.field(
        default=attrs.Factory(
            lambda self: BackgroundLoopControl(name=f"queue_consumer:{self.queue}"),
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
        """Stop the consumer at its next message boundary. Idempotent.

        Cancelling a consumer mid-handler rolls its transaction back and leaves the
        message unacked, so the next process redelivers it. Stopping between messages
        costs nothing.
        """

        return await self.control.stop(deadline=deadline)

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.restart_backoff.total_seconds() <= 0:
            raise exc.configuration("Restart backoff must be positive")

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        # Built eagerly at startup (not inside the detached task), so a broken
        # factory fails startup loudly instead of spawning a task that dies.
        consumer = self.consumer_factory(ctx)

        async def _loop() -> None:
            while True:
                try:
                    # timeout=None: consume forever. Per-message failures are
                    # absorbed inside the consumer's decision ladder; only a
                    # consume-generator crash (broker connection loss, ...)
                    # escapes to here. The stop signal ends the run at a message
                    # boundary, so shutdown never cancels a handler mid-flight.
                    await consumer.run(ctx, timeout=None, stop=self.control.event)

                except asyncio.CancelledError:
                    raise

                except Exception:
                    logger.exception(
                        "Queue consumer for %s crashed; restarting after backoff",
                        self.queue,
                    )

                if self.control.stopping:
                    # The run ended because we asked it to, not because it broke.
                    return

                # Reached on crash — or if the consume generator ever ends
                # despite timeout=None: restart either way after the backoff
                # so a flapping broker cannot hot-loop the consumer.
                # Jittered restart: a downstream outage crashes every
                # replica's consumer at once; without jitter they all restart
                # (and re-crash) in lockstep.
                if await self.control.sleep_or_stop(
                    # Desynchronization jitter, not security randomness.
                    self.restart_backoff.total_seconds()
                    * current_entropy_source().as_random().uniform(1.0, 1.5)
                ):
                    return

        if self.control.running:
            # The runtime invokes startup once per scope; a direct double call
            # must not leak (and orphan) the previous consumer task.
            logger.warning(
                "Queue consumer for %s already running; ignoring duplicate startup",
                self.queue,
            )
            return

        self.control.arm()
        self.control.task = asyncio.create_task(_loop(), name=self.control.loop_name)
        ctx.drainables.register(self)


# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class _QueueConsumerBackgroundShutdown(LifecycleHook):
    """Stop the background queue consumer.

    Normally a no-op — the runtime stops every registered loop before teardown begins. This is
    the fallback for a hand-driven lifecycle; ``stop`` is idempotent.
    """

    startup: _QueueConsumerBackgroundStartup
    """Startup hook."""

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        clock = asyncio.get_running_loop()
        await self.startup.stop(deadline=clock.time() + _STOP_GRACE_SECONDS)


# ....................... #


def queue_consumer_factory_background_lifecycle_step(
    *,
    queue: str,
    consumer_factory: Callable[[ExecutionContext], QueueConsumer[Any]],
    restart_backoff: timedelta = timedelta(seconds=5),
    step_id: StrKey | None = None,
) -> LifecycleStep:
    """Background queue-consumer step whose consumer is built from the scope's context.

    Same loop and crash/restart semantics as
    :func:`queue_consumer_background_lifecycle_step`, but the
    :class:`~forze_kits.integrations.consumer.QueueConsumer` is produced by
    *consumer_factory* at startup — for handlers that must resolve ports off the
    execution context (an index-maintenance consumer, a projector). The factory
    runs once per startup, eagerly, so a broken wiring fails startup loudly.
    """

    startup = _QueueConsumerBackgroundStartup(
        queue=queue,
        consumer_factory=consumer_factory,
        restart_backoff=restart_backoff,
    )
    shutdown = _QueueConsumerBackgroundShutdown(startup=startup)

    return LifecycleStep(
        id=step_id if step_id is not None else f"queue_consumer:{queue}",
        startup=startup,
        shutdown=shutdown,
    )


# ....................... #


def queue_consumer_background_lifecycle_step(
    *,
    queue: str,
    queue_spec: QueueSpec[Any],
    handler: Callable[[QueueMessage[Any]], Awaitable[None]],
    inbox_spec: InboxSpec,
    tx_route: StrKey,
    message_id: Callable[[QueueMessage[Any]], str] | None = None,
    bind_tenant_from_headers: bool = False,
    max_deliveries: int | None = None,
    retry_policy: StrKey | None = None,
    restart_backoff: timedelta = timedelta(seconds=5),
    step_id: StrKey | None = None,
) -> LifecycleStep:
    """Build a lifecycle step that runs a :class:`~forze_kits.integrations.consumer.QueueConsumer` in the background.

    Startup spawns a consume-forever task (``timeout=None``); shutdown
    cancels and awaits it. Per-message failures never reach this loop —
    they are handled inside the consumer's decision ladder (see
    :class:`~forze_kits.integrations.consumer.QueueConsumer`). A crash of the
    consume stream itself (broker connection loss, channel teardown) is
    logged and the consume restarts after *restart_backoff*; unacked
    in-flight messages are redelivered by the broker and deduped by the
    inbox.

    One step consumes one queue. For multiple queues — or multiple
    consumers on one queue across processes — register multiple steps with
    distinct *step_id* values (the default ``queue_consumer:<queue>`` keeps
    per-queue steps unique). There is deliberately no in-process concurrency
    knob in v1: a single sequential consumer per step keeps ordering and
    failure semantics simple; scale out with more steps/processes.

    Opt-in for long-running processes; all runner parameters
    (*max_deliveries*, *retry_policy*, *bind_tenant_from_headers*, ...)
    pass through with the same defaults and caveats.
    """

    consumer = QueueConsumer(
        queue=queue,
        queue_spec=queue_spec,
        handler=handler,
        inbox_spec=inbox_spec,
        tx_route=tx_route,
        message_id=message_id,
        bind_tenant_from_headers=bind_tenant_from_headers,
        max_deliveries=max_deliveries,
        retry_policy=retry_policy,
    )
    return queue_consumer_factory_background_lifecycle_step(
        queue=queue,
        consumer_factory=lambda ctx: consumer,
        restart_backoff=restart_backoff,
        step_id=step_id,
    )
