"""Canonical specs for the realtime stream, durable outbox route, and dedup inbox.

:func:`build_realtime_transport` is the **single source of truth**: build one
:class:`RealtimeTransport` from a channel name and hand each spec to the matching
component, so the publisher, relay, gateway, and dedup can never drift on channel
or codec. ``RealtimeSignal`` is a plain pydantic model, so the standard
:class:`PydanticModelCodec` serialises it — one shared instance, no custom codec.
"""

from typing import Final, final

import attrs

from forze.application.contracts.inbox import InboxSpec
from forze.application.contracts.outbox import OutboxDestination, OutboxSpec
from forze.application.contracts.realtime import RealtimeSignal
from forze.application.contracts.stream import StreamSpec
from forze.base.serialization import PydanticModelCodec

# ----------------------- #

DEFAULT_REALTIME_CHANNEL: Final[str] = "realtime"
"""Default stream name / outbox route for realtime signals."""

_REALTIME_CODEC: Final = PydanticModelCodec(model_type=RealtimeSignal)
"""Shared, stateless codec for the realtime signal — reused by every spec."""


# ....................... #


def realtime_stream_spec(name: str = DEFAULT_REALTIME_CHANNEL) -> StreamSpec[RealtimeSignal]:
    """Build the stream spec realtime signals are appended to and read from.

    Wire the same spec into the publish surface and the gateway so they share the
    channel and codec. Keep the stream **tenant-global**: the tenant rides in the
    message headers and isolation is enforced at room membership, so the gateway
    can consume every tenant's signals from one stream.
    """

    return StreamSpec(name=name, codec=_REALTIME_CODEC)


# ....................... #


def realtime_outbox_spec(
    name: str = DEFAULT_REALTIME_CHANNEL,
    *,
    stream: str = DEFAULT_REALTIME_CHANNEL,
) -> OutboxSpec[RealtimeSignal]:
    """Build the outbox route for durable realtime signals, relayed to *stream*.

    Durable signals are staged here in the transaction and the relay appends them
    to the realtime stream after commit.
    """

    return OutboxSpec(
        name=name,
        codec=_REALTIME_CODEC,
        destination=OutboxDestination.stream(route=stream, channel=stream),
    )


# ....................... #


def realtime_inbox_spec(name: str = f"{DEFAULT_REALTIME_CHANNEL}-inbox") -> InboxSpec:
    """Build the inbox route the gateway uses to dedupe durable signals (exactly-once).

    The default derives from :data:`DEFAULT_REALTIME_CHANNEL`; for a custom channel
    use :func:`build_realtime_transport`, which keeps the inbox name in lock-step.
    """

    return InboxSpec(name=name)


# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class RealtimeTransport:
    """The realtime specs for one channel — the single source of truth.

    Build once via :func:`build_realtime_transport` and pass each spec to the
    matching component: ``stream_spec`` → publisher + gateway source, ``outbox_spec``
    → publisher + relay, ``inbox_spec`` → gateway dedup. They can never drift.
    """

    stream_spec: StreamSpec[RealtimeSignal]
    """The realtime stream (publisher appends, gateway consumes)."""

    outbox_spec: OutboxSpec[RealtimeSignal]
    """The durable outbox route, relayed to the stream after commit."""

    inbox_spec: InboxSpec
    """The dedup inbox for exactly-once durable delivery at the gateway."""


# ....................... #


def build_realtime_transport(channel: str = DEFAULT_REALTIME_CHANNEL) -> RealtimeTransport:
    """Build every realtime spec for *channel* from one name, so they stay in lock-step."""

    return RealtimeTransport(
        stream_spec=realtime_stream_spec(channel),
        outbox_spec=realtime_outbox_spec(channel, stream=channel),
        inbox_spec=realtime_inbox_spec(f"{channel}-inbox"),
    )
