"""Lifecycle helpers for background outbox relay."""

from __future__ import annotations

import asyncio
from contextlib import suppress
from datetime import timedelta
from typing import Any, final

import attrs

from forze.application._logger import logger
from forze.application.contracts.execution import LifecycleHook, LifecycleStep
from forze.application.contracts.outbox import OutboxDestinationKind, OutboxSpec
from forze.application.contracts.pubsub import PubSubSpec
from forze.application.contracts.queue import QueueSpec
from forze.application.contracts.stream import StreamSpec
from forze.application.execution.context import ExecutionContext
from forze.base.exceptions import exc
from forze.base.primitives import StrKey

from .relay import relay_outbox, relay_outbox_to_queue

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True)
class _OutboxRelayBackgroundStartup(LifecycleHook):
    """Start a background task that periodically relays outbox rows."""

    outbox_spec: OutboxSpec[Any]
    transport: OutboxDestinationKind
    queue_spec: QueueSpec[Any] | None
    stream_spec: StreamSpec[Any] | None
    pubsub_spec: PubSubSpec[Any] | None
    interval: timedelta
    reclaim_stale_after: timedelta | None
    limit: int | None
    task: asyncio.Task[None] | None = attrs.field(default=None, init=False)

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.interval.total_seconds() <= 0:
            raise exc.configuration("Interval must be positive")

        if (
            self.reclaim_stale_after is not None
            and self.reclaim_stale_after.total_seconds() <= 0
        ):
            raise exc.configuration("Reclaim stale after must be positive")

    # ....................... #

    async def _relay_once(self, ctx: ExecutionContext) -> None:
        if self.transport == "queue":
            if self.queue_spec is None:
                raise exc.precondition(
                    "queue_spec is required for queue background relay"
                )

            await relay_outbox_to_queue(
                ctx,
                outbox_spec=self.outbox_spec,
                queue_spec=self.queue_spec,
                limit=self.limit,
                reclaim_stale_after=self.reclaim_stale_after,
            )
            return

        await relay_outbox(
            ctx,
            outbox_spec=self.outbox_spec,
            queue_spec=self.queue_spec,
            stream_spec=self.stream_spec,
            pubsub_spec=self.pubsub_spec,
            limit=self.limit,
            reclaim_stale_after=self.reclaim_stale_after,
        )

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        async def _loop() -> None:
            while True:
                try:
                    await self._relay_once(ctx)
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
    transport: OutboxDestinationKind = "queue",
    queue_spec: QueueSpec[Any] | None = None,
    stream_spec: StreamSpec[Any] | None = None,
    pubsub_spec: PubSubSpec[Any] | None = None,
    interval: timedelta = timedelta(seconds=30),
    reclaim_stale_after: timedelta | None = timedelta(minutes=5),
    limit: int | None = None,
    step_id: StrKey = "outbox_relay",
) -> LifecycleStep:
    """Build a lifecycle step that relays outbox rows on a background interval.

    *transport* selects which relay function runs each tick (default ``queue``).
    Pass the matching spec for the transport. For ``queue``, *queue_spec* is required
    unless :attr:`~forze.application.contracts.outbox.OutboxSpec.destination` is unset
    and you only use :func:`~forze_kits.integrations.outbox.relay_outbox_to_queue`
    semantics via explicit *queue_spec*.

    Opt-in for long-running processes. Production deployments often prefer
    external cron or workflow schedulers instead of in-process polling.
    """

    if transport == "queue" and queue_spec is None:
        raise exc.precondition("queue_spec is required when transport is queue")

    if transport == "stream" and stream_spec is None:
        raise exc.precondition("stream_spec is required when transport is stream")

    if transport == "pubsub" and pubsub_spec is None:
        raise exc.precondition("pubsub_spec is required when transport is pubsub")

    startup = _OutboxRelayBackgroundStartup(
        outbox_spec=outbox_spec,
        transport=transport,
        queue_spec=queue_spec,
        stream_spec=stream_spec,
        pubsub_spec=pubsub_spec,
        interval=interval,
        reclaim_stale_after=reclaim_stale_after,
        limit=limit,
    )
    shutdown = _OutboxRelayBackgroundShutdown(startup=startup)

    return LifecycleStep(id=step_id, startup=startup, shutdown=shutdown)
