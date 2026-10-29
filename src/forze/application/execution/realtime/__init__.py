"""Transport-neutral realtime egress: signal sources, the durable store half, its counters.

Every consumer of the realtime stream reads it through the same sources and decides what
reaches the offline mailbox through the same :class:`RealtimeDelivery`, so the Socket.IO
gateway and :class:`RealtimeMailboxWriter` (the mailbox writer for SSE- and WebSocket-only
deployments) cannot drift. It sits in the execution layer because it needs the
:class:`~forze.application.execution.ExecutionContext`, the drain gate and supervision.
"""

from .delivery import (
    DedupInboxConsumer,
    GatewayDedup,
    LiveEmit,
    RealtimeDelivery,
    claim_dedup_inbox,
)
from .observability import RealtimeGatewayStats, instrument_realtime_gateway
from .sources import (
    PubSubSignalSource,
    RealtimeSignalSource,
    SignalHandler,
    StreamGroupSignalSource,
    TenantShardedSignalSource,
    require_source_tenancy,
)
from .writer import RealtimeMailboxWriter, realtime_mailbox_writer_lifecycle_step

# ----------------------- #

__all__ = [
    "DedupInboxConsumer",
    "GatewayDedup",
    "LiveEmit",
    "PubSubSignalSource",
    "RealtimeDelivery",
    "RealtimeGatewayStats",
    "RealtimeMailboxWriter",
    "RealtimeSignalSource",
    "SignalHandler",
    "StreamGroupSignalSource",
    "TenantShardedSignalSource",
    "claim_dedup_inbox",
    "instrument_realtime_gateway",
    "realtime_mailbox_writer_lifecycle_step",
    "require_source_tenancy",
]
