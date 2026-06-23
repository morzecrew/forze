"""The realtime shard — one gateway instance's tenant assignment (RFC 0007).

The namespace-tier realtime plane has three sharded components that must agree on *which*
tenants, *which* stream, and *which* consumer group: the gateway's
:class:`~forze_socketio.TenantShardedSignalSource`, the group-ensure step, and the durable
relay step. Passing the assignment to each by hand invites drift — get one wrong and a tenant
is silently half-served (relayed but not consumed, or consumed but its group never ensured).

:class:`RealtimeShard` bundles the assignment into one value object handed to all three, so
drift is structurally impossible: one instance owns a disjoint shard end to end.
"""

from collections.abc import Callable, Sequence
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

    tenants: Callable[[], Sequence[UUID]]
    """This instance's assigned tenant shard, evaluated by each component as it needs it."""

    group: str = DEFAULT_REALTIME_GROUP
    """Consumer group name the gateway reads and the ensure step creates."""
