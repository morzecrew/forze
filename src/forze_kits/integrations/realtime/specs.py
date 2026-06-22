"""Canonical specs for the realtime stream and its durable outbox route.

The same ``(name, codec)`` pair is used by the publish surface and the transport
gateway, so they agree on the channel and the wire format. ``RealtimeSignal`` is
a plain pydantic model, so the standard :class:`PydanticModelCodec` serialises
it — no custom codec.
"""

from typing import Final

from forze.application.contracts.inbox import InboxSpec
from forze.application.contracts.outbox import OutboxDestination, OutboxSpec
from forze.application.contracts.realtime import RealtimeSignal
from forze.application.contracts.stream import StreamSpec
from forze.base.serialization import PydanticModelCodec

# ----------------------- #

DEFAULT_REALTIME_CHANNEL: Final[str] = "realtime"
"""Default stream name / outbox route for realtime signals."""


# ....................... #


def realtime_stream_spec(name: str = DEFAULT_REALTIME_CHANNEL) -> StreamSpec[RealtimeSignal]:
    """Build the stream spec realtime signals are appended to and read from.

    Wire the same spec into the publish surface and the gateway so they share the
    channel and codec. Keep the stream **tenant-global**: the tenant rides in the
    message headers and isolation is enforced at room membership, so the gateway
    can consume every tenant's signals from one stream.
    """

    return StreamSpec(name=name, codec=PydanticModelCodec(model_type=RealtimeSignal))


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
        codec=PydanticModelCodec(model_type=RealtimeSignal),
        destination=OutboxDestination.stream(route=stream, channel=stream),
    )


# ....................... #


def realtime_inbox_spec(name: str = "realtime-inbox") -> InboxSpec:
    """Build the inbox route the gateway uses to dedupe durable signals (exactly-once)."""

    return InboxSpec(name=name)
