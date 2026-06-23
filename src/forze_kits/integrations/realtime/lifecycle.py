"""Background wiring for durable realtime signals — relay the outbox to the stream.

Durable signals staged via :meth:`RealtimePublisher.stage` land in the outbox.
This step relays them to the realtime stream after commit, where the gateway
consumes them. A thin convenience over the generic
:func:`outbox_relay_background_lifecycle_step`.
"""

from collections.abc import Callable, Sequence
from datetime import timedelta
from typing import Any, final
from uuid import UUID

import attrs

from forze.application.contracts.execution import LifecycleHook, LifecycleStep
from forze.application.contracts.outbox import OutboxSpec
from forze.application.contracts.realtime import DEFAULT_REALTIME_GROUP
from forze.application.contracts.stream import StreamGroupAdminDepKey, StreamSpec
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import ExecutionContext
from forze.base.primitives import StrKey
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
            StreamGroupAdminDepKey,
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
        startup=_EnsureGroupStartup(
            stream_spec=stream_spec, group=group, start_id=start_id
        ),
        mutates_shared_state=True,
    )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class _EnsureTenantGroupsStartup(LifecycleHook):
    """Idempotently create the gateway's consumer group on **each** assigned tenant's
    realtime stream (namespace-tier; RFC 0007)."""

    stream_spec: StreamSpec[Any]
    tenants: Callable[[], Sequence[UUID]]
    group: str
    start_id: str

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        # Sequential bind/ensure per tenant (a startup hook, not the hot path): under
        # each bound tenant the admin port resolves to that tenant's stream key/partition.
        for tenant in self.tenants():
            with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=tenant)):
                admin = ctx.deps.resolve_configurable(
                    ctx,
                    StreamGroupAdminDepKey,
                    self.stream_spec,
                    route=self.stream_spec.name,
                )
                await admin.ensure_group(
                    self.group,
                    str(self.stream_spec.name),
                    start_id=self.start_id,
                )


# ....................... #


def realtime_tenant_group_ensure_lifecycle_step(
    *,
    stream_spec: StreamSpec[Any],
    tenants: Callable[[], Sequence[UUID]],
    group: str = DEFAULT_REALTIME_GROUP,
    start_id: str = "$",
    step_id: StrKey = "realtime_tenant_groups",
) -> LifecycleStep:
    """Create the gateway's consumer group on **each** assigned tenant's realtime stream.

    The namespace-tier counterpart of :func:`realtime_group_ensure_lifecycle_step`, paired
    with :class:`~forze_socketio.TenantShardedSignalSource`: the realtime stream route is
    wired ``tenant_aware``, so ensuring the group under each bound tenant creates it on that
    tenant's per-tenant stream key/partition. *tenants* must list the same shard the sharded
    source consumes. Order it **before** the relay and before serving (a fresh group's ``"$"``
    start would otherwise miss durable signals produced in the startup window). Idempotent;
    creates shared infrastructure, so under a ``FLEET`` profile wrap it with a singleton step.
    """

    return LifecycleStep(
        id=step_id,
        startup=_EnsureTenantGroupsStartup(
            stream_spec=stream_spec, tenants=tenants, group=group, start_id=start_id
        ),
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
    outbox_spec: OutboxSpec[Any],
    stream_spec: StreamSpec[Any],
    tenants: Callable[[], Sequence[UUID]],
    interval: timedelta = timedelta(seconds=2),
    step_id: StrKey = "realtime_tenant_relay",
) -> LifecycleStep:
    """Relay durable realtime signals **per assigned tenant** — the namespace-tier on-ramp.

    The namespace-tier counterpart of :func:`realtime_relay_lifecycle_step`, paired with
    :class:`~forze_socketio.TenantShardedSignalSource` and
    :func:`realtime_tenant_group_ensure_lifecycle_step`: when the realtime **outbox** is
    wired ``tenant_aware`` (partitioned), the relay must drain each tenant's partition under
    a bound tenant. Each tick relays every assigned tenant's staged signals to that tenant's
    stream key. Pass the **same** *tenants* shard this instance's gateway consumes (one
    instance owns a tenant shard end-to-end: ensure-group → relay → gateway). Keep the outbox
    tenant-global and use :func:`realtime_relay_lifecycle_step` instead when you don't need a
    physically-partitioned outbox.
    """

    return outbox_relay_background_lifecycle_step(
        outbox_spec=outbox_spec,
        transport="stream",
        stream_spec=stream_spec,
        tenants=tenants,
        interval=interval,
        step_id=step_id,
    )
