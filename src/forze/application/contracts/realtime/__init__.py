"""Realtime message contract — the data shared by the publish side and the gateway.

This is a **data contract only** — value objects and a typed event catalog, no
port and no `ctx.realtime` accessor. The application publishes a
:class:`RealtimeSignal` (addressed by a logical :class:`Audience`, typed by a
declared :class:`RealtimeEvent`) onto a messaging substrate; a transport gateway
(e.g. ``forze_socketio``) consumes it and bridges to live connections. Both sides
import this contract; neither owns a connection-reaching port.
"""

from .audience import Audience, AudienceKind
from .constants import DEFAULT_REALTIME_GROUP
from .events import RealtimeEvent, RealtimeEventCatalog
from .mailbox import MailboxEntry
from .shard import RealtimeShard
from .signal import RealtimeSignal

# ----------------------- #

__all__ = [
    "Audience",
    "AudienceKind",
    "DEFAULT_REALTIME_GROUP",
    "MailboxEntry",
    "RealtimeEvent",
    "RealtimeEventCatalog",
    "RealtimeShard",
    "RealtimeSignal",
]
