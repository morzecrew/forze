"""Realtime push contract — server→client emit addressed by a logical audience.

The :class:`RealtimePort` (resolved as ``ctx.realtime()``) lets any handler or
saga push to live clients; :class:`Audience` is the transport-neutral,
tenant-agnostic addressing vocabulary. A transport adapter (e.g.
``forze_socketio``) provides the implementation and applies tenant scoping.
"""

from .audience import Audience, AudienceKind
from .deps import RealtimeDepKey, RealtimeDepPort, RealtimeDeps
from .ports import RealtimePort

# ----------------------- #

__all__ = [
    "Audience",
    "AudienceKind",
    "RealtimePort",
    "RealtimeDepKey",
    "RealtimeDepPort",
    "RealtimeDeps",
]
