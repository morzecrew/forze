"""The realtime shard — one gateway instance's tenant assignment.

The namespace-tier realtime plane has three sharded components that must agree on *which*
tenants, *which* stream, and *which* consumer group: the gateway's
:class:`~forze_socketio.TenantShardedSignalSource`, the group-ensure step, and the durable
relay step. Passing the assignment to each by hand invites drift — get one wrong and a tenant
is silently half-served (relayed but not consumed, or consumed but its group never ensured).

:class:`RealtimeShard` bundles the assignment into one value object handed to all three, so
drift is structurally impossible: one instance owns a disjoint shard end to end.
"""

from typing import final
from uuid import UUID

import attrs

from forze.application.contracts.stream import StreamSpec

from .constants import DEFAULT_REALTIME_GROUP
from .signal import RealtimeSignal

# ----------------------- #


@final
@attrs.define(frozen=True, kw_only=True, slots=True)
class RealtimeShard:
    """One gateway instance's tenant shard for the namespace-tier realtime plane.

    Construct one and hand the same object to every sharded component
    (:class:`~forze_socketio.TenantShardedSignalSource`,
    :func:`~forze_kits.integrations.realtime.realtime_tenant_group_ensure_lifecycle_step`,
    :func:`~forze_kits.integrations.realtime.realtime_tenant_relay_lifecycle_step`) so they
    can never drift on the tenants, stream, or group.
    """

    stream_spec: StreamSpec[RealtimeSignal]
    """The per-tenant realtime stream (wired ``tenant_aware``) that all three components share."""

    tenants: tuple[UUID, ...] = attrs.field(converter=tuple)
    """This instance's assigned tenant shard — a **fixed snapshot**, resolved once where the
    shard is built (e.g. ``tenants=load_assigned_shard()``), not a provider each component
    re-reads. Storing the resolved assignment is what keeps the three components from drifting:
    a provider read independently during a rollout could hand them divergent tenant sets, so a
    tenant ends up consumed without its group ensured. Rebalancing a running fleet is out of
    scope — repartition by restart, which re-snapshots here."""

    group: str = DEFAULT_REALTIME_GROUP
    """Consumer group name the gateway reads and the ensure step creates."""
