"""Realtime egress over SSE — the second transport consumer of the egress plane.

The app side is unchanged: handlers publish ``RealtimeSignal`` s through the
messaging ports and never learn a transport exists. This package consumes the
same plane the Socket.IO gateway does — the mailbox for durable replay, a plain
broadcast stream tail for the live leg — and serves it as ``text/event-stream``
with the same versioned ``{id, data}`` envelope.
"""

from .hub import RealtimeSseHub, SseSubscription, presence_rooms
from .lifecycle import (
    realtime_sse_presence_heartbeat_lifecycle_step,
    realtime_sse_sharded_tail_lifecycle_step,
    realtime_sse_tail_lifecycle_step,
    refresh_sse_presence,
)
from .sse import (
    LAST_EVENT_ID_HEADER,
    RealtimeAckBody,
    RealtimeAckResult,
    TopicAuthorizer,
    attach_realtime_sse_route,
)

# ----------------------- #

__all__ = [
    "RealtimeSseHub",
    "SseSubscription",
    "presence_rooms",
    "realtime_sse_tail_lifecycle_step",
    "realtime_sse_sharded_tail_lifecycle_step",
    "realtime_sse_presence_heartbeat_lifecycle_step",
    "refresh_sse_presence",
    "attach_realtime_sse_route",
    "TopicAuthorizer",
    "RealtimeAckBody",
    "RealtimeAckResult",
    "LAST_EVENT_ID_HEADER",
]
