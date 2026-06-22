"""Realtime publish surface — emit signals onto messaging, never to a connection.

The application publishes a :class:`~forze.application.contracts.realtime.RealtimeSignal`
through :class:`RealtimePublisher` — ephemeral (stream, fire-and-forget) or durable
(outbox, relayed after commit) — and a transport gateway (e.g. ``forze_socketio``)
consumes the stream and bridges to live connections. Sibling of the ``notify`` kit.
"""

from .lifecycle import realtime_relay_lifecycle_step
from .publisher import RealtimePublisher
from .specs import (
    DEFAULT_REALTIME_CHANNEL,
    realtime_inbox_spec,
    realtime_outbox_spec,
    realtime_stream_spec,
)

# ----------------------- #

__all__ = [
    "RealtimePublisher",
    "DEFAULT_REALTIME_CHANNEL",
    "realtime_stream_spec",
    "realtime_outbox_spec",
    "realtime_inbox_spec",
    "realtime_relay_lifecycle_step",
]
