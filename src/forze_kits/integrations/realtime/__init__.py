"""Realtime publish surface — emit signals onto messaging, never to a connection.

The application publishes a :class:`~forze.application.contracts.realtime.RealtimeSignal`
through :class:`RealtimePublisher` — ephemeral (stream, fire-and-forget) or durable
(outbox, relayed after commit) — and a transport gateway (e.g. ``forze_socketio``)
consumes the stream and bridges to live connections. Sibling of the ``notify`` kit.
"""

from forze.application.contracts.realtime import RealtimeShard

from .lifecycle import (
    realtime_group_ensure_lifecycle_step,
    realtime_relay_lifecycle_step,
    realtime_stream_trim_lifecycle_step,
    realtime_tenant_group_ensure_lifecycle_step,
    realtime_tenant_relay_lifecycle_step,
)
from .mailbox import (
    DocumentMailboxCursors,
    DocumentRealtimeMailbox,
    MailboxStats,
    build_realtime_cursors,
    build_realtime_mailbox,
    realtime_cursor_spec,
    realtime_mailbox_spec,
)
from .observability import instrument_realtime_mailbox
from .publisher import (
    RealtimePublisher,
    RealtimePubSubPublisher,
    build_realtime_publisher,
    build_realtime_pubsub_publisher,
)
from .specs import (
    DEFAULT_REALTIME_CHANNEL,
    DEFAULT_REALTIME_STREAM_MAX_ENTRIES,
    RealtimeTransport,
    build_realtime_transport,
    realtime_inbox_spec,
    realtime_outbox_spec,
    realtime_pubsub_spec,
    realtime_stream_spec,
)

# ----------------------- #

__all__ = [
    "RealtimePublisher",
    "RealtimePubSubPublisher",
    "build_realtime_publisher",
    "build_realtime_pubsub_publisher",
    "realtime_pubsub_spec",
    "DocumentRealtimeMailbox",
    "DocumentMailboxCursors",
    "build_realtime_mailbox",
    "build_realtime_cursors",
    "MailboxStats",
    "instrument_realtime_mailbox",
    "realtime_mailbox_spec",
    "realtime_cursor_spec",
    "RealtimeTransport",
    "build_realtime_transport",
    "DEFAULT_REALTIME_CHANNEL",
    "DEFAULT_REALTIME_STREAM_MAX_ENTRIES",
    "realtime_stream_spec",
    "realtime_outbox_spec",
    "realtime_inbox_spec",
    "realtime_relay_lifecycle_step",
    "realtime_stream_trim_lifecycle_step",
    "realtime_group_ensure_lifecycle_step",
    "realtime_tenant_group_ensure_lifecycle_step",
    "realtime_tenant_relay_lifecycle_step",
    "RealtimeShard",
]
