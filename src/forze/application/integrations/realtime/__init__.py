"""Transport-neutral realtime connection kernel.

The pieces of a realtime connection that are the same on every transport: the
offline mailbox + per-device cursor seams (with in-memory implementations), the
replay/cumulative-ack discipline over them, the client-key ladder, and the wire
protocol version. The Socket.IO connection layer and the SSE egress route both
consume this kernel — neither may import the other, and adding a transport must
never add a delivery contract.
"""

from .mailbox import (
    InMemoryMailboxCursors,
    InMemoryRealtimeMailbox,
    MailboxCursors,
    MailboxEntry,
    RealtimeMailbox,
)
from .protocol import (
    REALTIME_PROTOCOL_VERSION,
    SUPPORTED_REALTIME_PROTOCOLS,
    negotiate_realtime_protocol,
)
from .replay import acknowledge_up_to, iter_replay, resolve_client_key

# ----------------------- #

__all__ = [
    "MailboxEntry",
    "RealtimeMailbox",
    "MailboxCursors",
    "InMemoryRealtimeMailbox",
    "InMemoryMailboxCursors",
    "REALTIME_PROTOCOL_VERSION",
    "SUPPORTED_REALTIME_PROTOCOLS",
    "negotiate_realtime_protocol",
    "acknowledge_up_to",
    "iter_replay",
    "resolve_client_key",
]
