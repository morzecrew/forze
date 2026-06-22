"""The stored-signal value object for the offline mailbox (data only).

A :class:`MailboxEntry` is one durable signal persisted for later replay — enough
to re-emit it to a device that reconnects. It lives in core (like
:class:`~forze.application.contracts.realtime.signal.RealtimeSignal`) so both the
gateway/connection side (``forze_socketio``, which defines the mailbox Protocols)
and the storage side (``forze_kits``, which implements them over the document
store) can share it without either importing the other.
"""

from typing import Any, final

import attrs

from forze.base.primitives import HlcTimestamp

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class MailboxEntry:
    """One stored durable signal, enough to re-emit it on replay."""

    event_id: str
    """The durable event id (``forze_event_id``) — replay dedup + ack cursor key."""

    hlc: HlcTimestamp
    """Causal-monotonic order/position; the per-device cursor advances over these."""

    event: str
    """The event name to emit."""

    payload: dict[str, Any]
    """The event payload to emit."""
