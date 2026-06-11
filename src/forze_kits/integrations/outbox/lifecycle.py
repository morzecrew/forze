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
from forze.application.contracts.outbox import OutboxRelayResult
from forze.base.exceptions import exc
from forze.base.primitives import StrKey

from ._relay_core import validate_retry_options
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
    max_attempts: int
    retry_base_delay: timedelta
    retry_max_backoff: timedelta
    max_batches_per_tick: int
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

        if self.max_batches_per_tick < 1:
            raise exc.configuration("Max batches per tick must be >= 1")

        validate_retry_options(
            max_attempts=self.max_attempts,
            retry_base_delay=self.retry_base_delay,
            retry_max_backoff=self.retry_max_backoff,
        )

    # ....................... #

    async def _relay_batch(
        self,
        ctx: ExecutionContext,
        *,
        reclaim_stale_after: timedelta | None,
    ) -> OutboxRelayResult:
        if self.transport == "queue":
            if self.queue_spec is None:
                raise exc.precondition(
                    "queue_spec is required for queue background relay"
                )

            return await relay_outbox_to_queue(
                ctx,
                outbox_spec=self.outbox_spec,
                queue_spec=self.queue_spec,
                limit=self.limit,
                reclaim_stale_after=reclaim_stale_after,
                max_attempts=self.max_attempts,
                retry_base_delay=self.retry_base_delay,
                retry_max_backoff=self.retry_max_backoff,
            )

        return await relay_outbox(
            ctx,
            outbox_spec=self.outbox_spec,
            queue_spec=self.queue_spec,
            stream_spec=self.stream_spec,
            pubsub_spec=self.pubsub_spec,
            limit=self.limit,
            reclaim_stale_after=reclaim_stale_after,
            max_attempts=self.max_attempts,
            retry_base_delay=self.retry_base_delay,
            retry_max_backoff=self.retry_max_backoff,
        )

    # ....................... #

    async def _relay_once(self, ctx: ExecutionContext) -> None:
        """Drain the backlog: relay batches until a claim comes back short.

        Stops when a batch claims fewer rows than the batch size (backlog
        drained) or after ``max_batches_per_tick`` batches (safety cap so a
        large backlog cannot starve the loop). A failing batch is logged and
        does not abort the tick. Stale-processing reclaim runs only with the
        first batch of the tick.
        """

        for batch_index in range(self.max_batches_per_tick):
            reclaim = self.reclaim_stale_after if batch_index == 0 else None

            try:
                result = await self._relay_batch(ctx, reclaim_stale_after=reclaim)

            except asyncio.CancelledError:
                raise

            except Exception:
                logger.exception("Outbox background relay batch failed")
                continue

            drained = result.claimed == 0 or (
                self.limit is not None and result.claimed < self.limit
            )

            if drained:
                break

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
    max_attempts: int = 5,
    retry_base_delay: timedelta = timedelta(seconds=1),
    retry_max_backoff: timedelta = timedelta(minutes=5),
    max_batches_per_tick: int = 100,
    step_id: StrKey = "outbox_relay",
) -> LifecycleStep:
    """Build a lifecycle step that relays outbox rows on a background interval.

    *transport* selects which relay function runs each tick (default ``queue``).
    Pass the matching spec for the transport. For ``queue``, *queue_spec* is required
    unless :attr:`~forze.application.contracts.outbox.OutboxSpec.destination` is unset
    and you only use :func:`~forze_kits.integrations.outbox.relay_outbox_to_queue`
    semantics via explicit *queue_spec*.

    Each tick **drains the backlog**: batches of up to *limit* rows are relayed
    until a claim returns fewer rows than the batch size, capped at
    *max_batches_per_tick* batches so a large backlog cannot starve the loop;
    then the task sleeps *interval*. A failing batch is logged and does not
    abort the tick.

    *max_attempts*, *retry_base_delay*, and *retry_max_backoff* configure the
    per-row transient-failure retry policy — see
    :func:`~forze_kits.integrations.outbox.relay_outbox_to_queue`. Delivery is
    at-least-once and ordering is not preserved across failures/retries.

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
        max_attempts=max_attempts,
        retry_base_delay=retry_base_delay,
        retry_max_backoff=retry_max_backoff,
        max_batches_per_tick=max_batches_per_tick,
    )
    shutdown = _OutboxRelayBackgroundShutdown(startup=startup)

    return LifecycleStep(id=step_id, startup=startup, shutdown=shutdown)
