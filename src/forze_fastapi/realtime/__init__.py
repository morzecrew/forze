"""Realtime egress over SSE — the second transport consumer of the egress plane.

The app side is unchanged: handlers publish ``RealtimeSignal`` s through the
messaging ports and never learn a transport exists. This package consumes the
same plane the Socket.IO gateway does — the mailbox for durable replay, a plain
broadcast stream tail for the live leg — and serves it as ``text/event-stream``
with the same versioned ``{id, data}`` envelope.
"""

from .hub import RealtimeSseHub, SseSubscription
from .lifecycle import realtime_sse_tail_lifecycle_step
from .sse import (
    LAST_EVENT_ID_HEADER,
    RealtimeAckBody,
    RealtimeAckResult,
    attach_realtime_sse_route,
)

# ----------------------- #

__all__ = [
    "RealtimeSseHub",
    "SseSubscription",
    "realtime_sse_tail_lifecycle_step",
    "attach_realtime_sse_route",
    "RealtimeAckBody",
    "RealtimeAckResult",
    "LAST_EVENT_ID_HEADER",
]
