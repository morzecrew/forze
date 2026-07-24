"""Transport-neutral realtime connection kernel.

The pieces of a realtime connection that are the same on every transport: the
offline mailbox + per-device cursor seams (with in-memory implementations), the
replay/cumulative-ack discipline over them, the client-key ladder, and the wire
protocol version. The Socket.IO connection layer and the SSE egress route both
consume this kernel — neither may import the other, and adding a transport must
never add a delivery contract.
"""

from .commands import RealtimeCommandRoute
from .frames import FRAME_UNSERIALIZABLE_CODE, encode_frame, jsonable_frame
from .mailbox import (
    InMemoryMailboxCursors,
    InMemoryRealtimeMailbox,
    MailboxCursors,
    MailboxEntry,
    RealtimeMailbox,
)
from .presence import InMemoryRealtimePresence, RealtimePresence
from .protocol import (
    REALTIME_PROTOCOL_VERSION,
    SUPPORTED_REALTIME_PROTOCOLS,
    RealtimeAck,
    negotiate_realtime_protocol,
)
from .replay import (
    BacklogDrain,
    acknowledge_up_to,
    iter_backlog,
    iter_replay,
    resolve_client_key,
)
from .rooms import room_for

# ----------------------- #

__all__ = [
    "MailboxEntry",
    "RealtimeMailbox",
    "MailboxCursors",
    "InMemoryRealtimeMailbox",
    "InMemoryMailboxCursors",
    "RealtimePresence",
    "InMemoryRealtimePresence",
    "REALTIME_PROTOCOL_VERSION",
    "SUPPORTED_REALTIME_PROTOCOLS",
    "RealtimeAck",
    "RealtimeCommandRoute",
    "negotiate_realtime_protocol",
    "acknowledge_up_to",
    "FRAME_UNSERIALIZABLE_CODE",
    "encode_frame",
    "jsonable_frame",
    "BacklogDrain",
    "iter_backlog",
    "iter_replay",
    "resolve_client_key",
    "room_for",
]
