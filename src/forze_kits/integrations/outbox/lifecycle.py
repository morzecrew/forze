"""Lifecycle helpers for background outbox relay."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Sequence
from contextlib import suppress
from datetime import timedelta
from typing import Any, final
from uuid import UUID

import attrs

from forze.application._logger import logger
from forze.application.contracts.execution import LifecycleHook, LifecycleStep
from forze.application.contracts.outbox import OutboxDestinationKind, OutboxSpec
from forze.application.contracts.pubsub import PubSubSpec
from forze.application.contracts.queue import QueueSpec
from forze.application.contracts.stream import StreamSpec
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution.context import ExecutionContext
from forze.application.contracts.outbox import OutboxRelayResult
from forze.base.exceptions import exc
from forze.base.primitives import StrKey, current_entropy_source

from ._relay_core import validate_retry_options
from .relay import OutboxRelay

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
    jitter: float = 0.2
    reclaim_stale_after: timedelta | None
    limit: int | None
    max_attempts: int
    retry_base_delay: timedelta
    retry_max_backoff: timedelta
    max_batches_per_tick: int
    tenants: Callable[[], Sequence[UUID]] | None = None
    """When set, the outbox is tenant-aware (partitioned): each tick drains every assigned
    tenant's partition under a bound tenant (namespace tier, RFC 0007). ``None`` = the
    outbox is tenant-global and one unbound pass drains it."""
    task: asyncio.Task[None] | None = attrs.field(default=None, init=False)

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.interval.total_seconds() <= 0:
            raise exc.configuration("Interval must be positive")

        if not 0.0 <= self.jitter < 1.0:
            raise exc.configuration("Jitter must be in [0, 1)")

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
        # Per-batch relay config: reclaim varies (only the first batch of a tick
        # reclaims stale rows), so build a fresh OutboxRelay per batch.
        relay = OutboxRelay(
            outbox_spec=self.outbox_spec,
            reclaim_stale_after=reclaim_stale_after,
            max_attempts=self.max_attempts,
            retry_base_delay=self.retry_base_delay,
            retry_max_backoff=self.retry_max_backoff,
        )

        if self.transport == "queue":
            if self.queue_spec is None:
                raise exc.precondition(
                    "queue_spec is required for queue background relay"
                )

            return await relay.to_queue(ctx, self.queue_spec, limit=self.limit)

        return await relay.run(
            ctx,
            queue_spec=self.queue_spec,
            stream_spec=self.stream_spec,
            pubsub_spec=self.pubsub_spec,
            limit=self.limit,
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

    async def _drain_tick(self, ctx: ExecutionContext) -> None:
        """Drain the backlog once: globally, or per assigned tenant when sharded.

        Sharded, each tenant's pass runs **bound** (so the relay reads/marks that tenant's
        partition and forwards under it) and is isolated — one tenant's failure must not
        skip the rest of the shard this tick. Tenants are drained **sequentially** (one DB
        connection at a time); shard across instances to parallelize.
        """

        if self.tenants is None:
            await self._relay_once(ctx)
            return

        for tenant in self.tenants():
            try:
                with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=tenant)):
                    await self._relay_once(ctx)

            except asyncio.CancelledError:
                raise

            except Exception:
                logger.exception("Outbox background relay failed for tenant", tenant=str(tenant))

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        async def _loop() -> None:
            while True:
                try:
                    await self._drain_tick(ctx)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("Outbox background relay failed")

                # Jittered tick: N replicas polling at the same fixed
                # interval synchronize into a thundering herd against the
                # claim query; the multiplicative jitter desynchronizes them.
                await asyncio.sleep(
                    self.interval.total_seconds()
                    # Desynchronization jitter, not security randomness.
                    * (
                        1.0
                        + current_entropy_source().as_random().uniform(
                            -self.jitter, self.jitter
                        )
                    )
                )

        if self.task is not None and not self.task.done():
            # The runtime invokes startup once per scope; a direct double call
            # must not leak (and orphan) the previous relay task.
            logger.warning("Outbox relay already running; ignoring duplicate startup")
            return

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
    jitter: float = 0.2,
    reclaim_stale_after: timedelta | None = timedelta(minutes=5),
    limit: int | None = None,
    max_attempts: int = 5,
    retry_base_delay: timedelta = timedelta(seconds=1),
    retry_max_backoff: timedelta = timedelta(minutes=5),
    max_batches_per_tick: int = 100,
    tenants: Callable[[], Sequence[UUID]] | None = None,
    step_id: StrKey = "outbox_relay",
) -> LifecycleStep:
    """Build a lifecycle step that relays outbox rows on a background interval.

    *transport* selects which :class:`~forze_kits.integrations.outbox.OutboxRelay` method
    runs each tick (default ``queue``). Pass the matching spec for the transport. For
    ``queue``, *queue_spec* is required unless
    :attr:`~forze.application.contracts.outbox.OutboxSpec.destination` is unset and you
    relay via explicit *queue_spec* (``OutboxRelay.to_queue`` semantics).

    Each tick **drains the backlog**: batches of up to *limit* rows are relayed
    until a claim returns fewer rows than the batch size, capped at
    *max_batches_per_tick* batches so a large backlog cannot starve the loop;
    then the task sleeps *interval* with multiplicative *jitter* (default
    ±20%) so N replicas' relay loops do not synchronize into a thundering
    herd against the claim query. A failing batch is logged and does not
    abort the tick.

    *max_attempts*, *retry_base_delay*, and *retry_max_backoff* configure the
    per-row transient-failure retry policy — see
    :class:`~forze_kits.integrations.outbox.OutboxRelay`. Delivery is
    at-least-once and ordering is not preserved across failures/retries.

    When *tenants* is set the outbox is **tenant-aware** (partitioned): each tick drains
    every assigned tenant's partition under a bound tenant, so the relay can read a
    namespace-tier outbox (which fail-closes when read unbound) and forward each row to its
    tenant's destination. Tenants drain **sequentially** per tick; assign the **same shard**
    this instance's gateway consumes, and shard across instances to parallelize. Omit it for
    a tenant-global (tagged) outbox, which one unbound pass drains with per-row routing.

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
        jitter=jitter,
        reclaim_stale_after=reclaim_stale_after,
        limit=limit,
        max_attempts=max_attempts,
        retry_base_delay=retry_base_delay,
        retry_max_backoff=retry_max_backoff,
        max_batches_per_tick=max_batches_per_tick,
        tenants=tenants,
    )
    shutdown = _OutboxRelayBackgroundShutdown(startup=startup)

    return LifecycleStep(id=step_id, startup=startup, shutdown=shutdown)
