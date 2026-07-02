"""Lifecycle helpers for the background queue consumer."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from contextlib import suppress
from datetime import timedelta
from typing import Any, final

import attrs

from forze.application.contracts.execution import LifecycleHook, LifecycleStep
from forze_kits.integrations._logger import logger
from forze.application.contracts.inbox import InboxSpec
from forze.application.contracts.queue import QueueMessage, QueueSpec
from forze.application.execution.context import ExecutionContext
from forze.base.exceptions import exc
from forze.base.primitives import StrKey, current_entropy_source

from .runner import QueueConsumer

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True)
class _QueueConsumerBackgroundStartup(LifecycleHook):
    """Start a background task that runs a :class:`QueueConsumer` forever."""

    consumer: QueueConsumer[Any]
    """The configured consumer; carries all consume options (validated on build)."""

    restart_backoff: timedelta
    task: asyncio.Task[None] | None = attrs.field(default=None, init=False)

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.restart_backoff.total_seconds() <= 0:
            raise exc.configuration("Restart backoff must be positive")

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        async def _loop() -> None:
            while True:
                try:
                    # timeout=None: consume forever. Per-message failures are
                    # absorbed inside the consumer's decision ladder; only a
                    # consume-generator crash (broker connection loss, ...)
                    # escapes to here.
                    await self.consumer.run(ctx, timeout=None)

                except asyncio.CancelledError:
                    raise

                except Exception:
                    logger.exception(
                        "Queue consumer for %s crashed; restarting after backoff",
                        self.consumer.queue,
                    )

                # Reached on crash — or if the consume generator ever ends
                # despite timeout=None: restart either way after the backoff
                # so a flapping broker cannot hot-loop the consumer.
                # Jittered restart: a downstream outage crashes every
                # replica's consumer at once; without jitter they all restart
                # (and re-crash) in lockstep.
                await asyncio.sleep(
                    # Desynchronization jitter, not security randomness.
                    self.restart_backoff.total_seconds()
                    * current_entropy_source().as_random().uniform(1.0, 1.5)
                )

        if self.task is not None and not self.task.done():
            # The runtime invokes startup once per scope; a direct double call
            # must not leak (and orphan) the previous consumer task.
            logger.warning(
                "Queue consumer for %s already running; ignoring duplicate startup",
                self.consumer.queue,
            )
            return

        self.task = asyncio.create_task(
            _loop(),
            name=f"queue_consumer:{self.consumer.queue}",
        )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class _QueueConsumerBackgroundShutdown(LifecycleHook):
    """Cancel the background queue consumer task."""

    startup: _QueueConsumerBackgroundStartup
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
    startup = _QueueConsumerBackgroundStartup(
        consumer=consumer,
        restart_backoff=restart_backoff,
    )
    shutdown = _QueueConsumerBackgroundShutdown(startup=startup)

    return LifecycleStep(
        id=step_id if step_id is not None else f"queue_consumer:{queue}",
        startup=startup,
        shutdown=shutdown,
    )
