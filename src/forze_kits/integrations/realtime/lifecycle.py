"""Background wiring for durable realtime signals — relay the outbox to the stream.

Durable signals staged via :meth:`RealtimePublisher.stage` land in the outbox.
This step relays them to the realtime stream after commit, where the gateway
consumes them. A thin convenience over the generic
:func:`outbox_relay_background_lifecycle_step`.
"""

import asyncio
from collections.abc import Callable, Sequence
from datetime import timedelta
from typing import Any, final
from uuid import UUID

import attrs

from forze.application.contracts.execution import LifecycleHook, LifecycleStep
from forze.application.contracts.outbox import OutboxSpec
from forze.application.contracts.realtime import DEFAULT_REALTIME_GROUP, RealtimeShard
from forze.application.contracts.stream import AckStreamGroupAdminDepKey, StreamSpec
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import ExecutionContext
from forze.application.execution.background import (
    DEFAULT_STOP_GRACE_SECONDS,
    BackgroundLoopControl,
)
from forze.base.exceptions import exc
from forze.base.primitives import StrKey, current_entropy_source
from forze_kits.integrations._logger import logger
from forze_kits.integrations.outbox import outbox_relay_background_lifecycle_step

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True)
class _EnsureGroupStartup(LifecycleHook):
    """Idempotently create the gateway's consumer group at startup."""

    stream_spec: StreamSpec[Any]
    group: str
    start_id: str

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        admin = ctx.deps.resolve_configurable(
            ctx,
            AckStreamGroupAdminDepKey,
            self.stream_spec,
            route=self.stream_spec.name,
        )
        await admin.ensure_group(
            self.group,
            str(self.stream_spec.name),
            start_id=self.start_id,
        )


# ....................... #


def realtime_group_ensure_lifecycle_step(
    *,
    stream_spec: StreamSpec[Any],
    group: str = DEFAULT_REALTIME_GROUP,
    start_id: str = "$",
    step_id: StrKey = "realtime_group",
) -> LifecycleStep:
    """Create the gateway's consumer group on the realtime stream at startup.

    Order this **before** the relay and before serving so a fresh group's ``"$"``
    start does not miss durable signals produced in the startup window. Idempotent
    (a no-op if the group exists). It creates shared infrastructure, so under a
    ``FLEET`` profile wrap it with a singleton lifecycle step.
    """

    return LifecycleStep(
        id=step_id,
        startup=_EnsureGroupStartup(stream_spec=stream_spec, group=group, start_id=start_id),
        mutates_shared_state=True,
    )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class _EnsureTenantGroupsStartup(LifecycleHook):
    """Idempotently create the gateway's consumer group on **each** assigned tenant's
    realtime stream (namespace-tier)."""

    shard: RealtimeShard
    """The realtime shard."""

    start_id: str
    """The start id to use."""

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        # Sequential bind/ensure per tenant (a startup hook, not the hot path): under
        # each bound tenant the admin port resolves to that tenant's stream key/partition.
        stream_spec = self.shard.stream_spec

        for tenant in self.shard.tenants:
            with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=tenant)):
                admin = ctx.deps.resolve_configurable(
                    ctx,
                    AckStreamGroupAdminDepKey,
                    stream_spec,
                    route=stream_spec.name,
                )
                await admin.ensure_group(
                    self.shard.group,
                    str(stream_spec.name),
                    start_id=self.start_id,
                )


# ....................... #


def realtime_tenant_group_ensure_lifecycle_step(
    *,
    shard: RealtimeShard,
    start_id: str = "$",
    step_id: StrKey = "realtime_tenant_groups",
) -> LifecycleStep:
    """Create the gateway's consumer group on **each** assigned tenant's realtime stream.

    The namespace-tier counterpart of :func:`realtime_group_ensure_lifecycle_step`, paired
    with :class:`~forze_socketio.TenantShardedSignalSource`: the realtime stream route is
    wired ``tenant_aware``, so ensuring the group under each bound tenant creates it on that
    tenant's per-tenant stream key/partition. Pass the **same** :class:`RealtimeShard` the
    sharded source and relay use, so they can't drift. Order it **before** the relay and
    before serving (a fresh group's ``"$"`` start would otherwise miss durable signals
    produced in the startup window). Idempotent; creates shared infrastructure, so under a
    ``FLEET`` profile wrap it with a singleton step.
    """

    return LifecycleStep(
        id=step_id,
        startup=_EnsureTenantGroupsStartup(shard=shard, start_id=start_id),
        mutates_shared_state=True,
    )


# ....................... #


def realtime_relay_lifecycle_step(
    *,
    outbox_spec: OutboxSpec[Any],
    stream_spec: StreamSpec[Any],
    interval: timedelta = timedelta(seconds=2),
    step_id: StrKey = "realtime_relay",
) -> LifecycleStep:
    """Relay durable realtime signals from the outbox to the realtime stream.

    Run alongside the gateway: ``stage`` → outbox → (this relay) → stream →
    gateway → emit. The signals carry the outbox event id, so the gateway dedupes
    them for exactly-once delivery.
    """

    return outbox_relay_background_lifecycle_step(
        outbox_spec=outbox_spec,
        transport="stream",
        stream_spec=stream_spec,
        interval=interval,
        step_id=step_id,
    )


# ....................... #


def realtime_tenant_relay_lifecycle_step(
    *,
    shard: RealtimeShard,
    outbox_spec: OutboxSpec[Any],
    interval: timedelta = timedelta(seconds=2),
    step_id: StrKey = "realtime_tenant_relay",
) -> LifecycleStep:
    """Relay durable realtime signals **per assigned tenant** — the namespace-tier on-ramp.

    The namespace-tier counterpart of :func:`realtime_relay_lifecycle_step`, paired with
    :class:`~forze_socketio.TenantShardedSignalSource` and
    :func:`realtime_tenant_group_ensure_lifecycle_step`: when the realtime **outbox** is
    wired ``tenant_aware`` (partitioned), the relay must drain each tenant's partition under
    a bound tenant. Each tick relays every assigned tenant's staged signals to that tenant's
    stream key. Pass the **same** :class:`RealtimeShard` this instance's gateway and
    group-ensure step use (one instance owns a tenant shard end to end). Keep the outbox
    tenant-global and use :func:`realtime_relay_lifecycle_step` instead when you don't need a
    physically-partitioned outbox.
    """

    return outbox_relay_background_lifecycle_step(
        outbox_spec=outbox_spec,
        transport="stream",
        stream_spec=shard.stream_spec,
        # the relay step takes a provider; hand it one that returns the shard's fixed
        # snapshot, so it sees the same tenant set as the gateway and group-ensure step
        tenants=lambda: shard.tenants,
        interval=interval,
        step_id=step_id,
    )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class _StreamTrimStartup(LifecycleHook):
    """Periodically trim the realtime stream below every group's acknowledged horizon."""

    stream_spec: StreamSpec[Any]
    interval: timedelta
    jitter: float
    tenants: Callable[[], Sequence[UUID]] | None

    control: BackgroundLoopControl = attrs.field(
        default=attrs.Factory(
            lambda self: BackgroundLoopControl(
                name=f"realtime_stream_trim:{self.stream_spec.name}"
            ),
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
        """Stop the loop between ticks. Idempotent — a trim is idempotent and monotonic,
        so even a tick cut mid-sweep costs nothing (the next sweep, anywhere, finishes it)."""

        return await self.control.stop(deadline=deadline)

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.interval.total_seconds() <= 0:
            raise exc.configuration("Trim interval must be positive")

        if not 0.0 <= self.jitter < 1.0:
            raise exc.configuration("Jitter must be in [0, 1)")

    # ....................... #

    async def _trim_tick(self, ctx: ExecutionContext, tenants: Sequence[UUID] | None) -> None:
        admin = ctx.deps.resolve_configurable(
            ctx,
            AckStreamGroupAdminDepKey,
            self.stream_spec,
            route=self.stream_spec.name,
        )

        if tenants is None:
            await admin.trim_acknowledged(str(self.stream_spec.name))
            return

        for tenant in tenants:
            try:
                with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=tenant)):
                    await admin.trim_acknowledged(str(self.stream_spec.name))

            except asyncio.CancelledError:
                raise

            except Exception:
                logger.exception("Realtime stream trim failed for tenant", tenant=str(tenant))

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        # Freeze the assigned tenant shard at startup (restart to repartition), matching the
        # sharded gateway/relay — a broken provider fails startup loudly, not as a dead task.
        tenants = list(self.tenants()) if self.tenants is not None else None

        if self.control.running:
            logger.warning("Realtime stream trim already running; ignoring duplicate startup")
            return

        self.control.arm()

        async def _loop() -> None:
            while True:
                try:
                    await self._trim_tick(ctx, tenants)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("Realtime stream trim sweep failed")

                # Multiplicative jitter desynchronizes N replicas' sweeps; racing trims are
                # harmless (the floor is monotonic), this only avoids redundant round trips.
                if await self.control.sleep_or_stop(
                    self.interval.total_seconds()
                    * (
                        1.0
                        + current_entropy_source().as_random().uniform(-self.jitter, self.jitter)
                    )
                ):
                    return

        self.control.task = asyncio.create_task(_loop(), name=self.control.loop_name)
        ctx.drainables.register(self)


# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class _StreamTrimShutdown(LifecycleHook):
    """Stop the trim loop.

    Normally a no-op — the runtime stops every registered loop before teardown begins. This
    is the fallback for a hand-driven lifecycle; ``stop`` is idempotent.
    """

    startup: _StreamTrimStartup

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        clock = asyncio.get_running_loop()
        await self.startup.stop(deadline=clock.time() + DEFAULT_STOP_GRACE_SECONDS)


# ....................... #


def realtime_stream_trim_lifecycle_step(
    *,
    stream_spec: StreamSpec[Any],
    interval: timedelta = timedelta(seconds=60),
    jitter: float = 0.2,
    tenants: Callable[[], Sequence[UUID]] | None = None,
    step_id: StrKey = "realtime_stream_trim",
) -> LifecycleStep:
    """Periodically trim the realtime stream below every group's **acknowledged** horizon.

    The precise companion to the blunt retention cap
    (``RedisStreamConfig.retention_max_entries``): each sweep calls
    ``AckStreamGroupAdminPort.trim_acknowledged``, which removes only entries every consumer
    group on the stream has been delivered *and* has acked — an undelivered or pending entry
    is never touched, so the sweep cannot outrun a slow or crashed gateway. Keep the cap as
    the backstop (a group nobody reads holds the floor forever; the cap still bounds the
    stream); run this to keep steady-state memory near the group's real horizon instead of
    the cap.

    Safe on any interval and on every node concurrently (the floor is monotonic; racing
    trims are wasted round trips, not hazards) — under a ``FLEET`` profile, wrap it with a
    singleton lifecycle step if you want exactly one sweeper.

    *tenants* mirrors the sharded gateway/relay: on a ``tenant_aware`` (namespace-tier)
    stream route, each sweep binds every assigned tenant in turn and trims its per-tenant
    stream key — pass ``lambda: shard.tenants`` from the same
    :class:`~forze.application.contracts.realtime.RealtimeShard` the other components use.
    ``None`` (the default) sweeps the tenant-global stream once.
    """

    startup = _StreamTrimStartup(
        stream_spec=stream_spec,
        interval=interval,
        jitter=jitter,
        tenants=tenants,
    )

    return LifecycleStep(
        id=step_id,
        startup=startup,
        shutdown=_StreamTrimShutdown(startup=startup),
        requires_long_running=True,
    )
