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
from forze.application.contracts.inventory import SpecRegistry, SpecSource
from forze.application.contracts.outbox import OutboxDestination, OutboxSpec
from forze.application.contracts.realtime import RealtimeSignal
from forze.application.contracts.stream import StreamSpec
from forze.base.serialization import PydanticModelCodec

# ----------------------- #

DEFAULT_REALTIME_CHANNEL: Final[str] = "realtime"
"""Default stream name / outbox route for realtime signals."""

DEFAULT_REALTIME_STREAM_MAX_ENTRIES: Final[int] = 100_000
"""Recommended retention cap for the realtime stream route.

Set it on the stream route's backend config (``RedisStreamConfig(retention_max_entries=...)``,
``MockRouteConfig(stream_retention_max_entries=...)``) — retention is a route concern the
specs cannot carry. The realtime stream is the one stream with an unbounded, framework-driven
producer (every ephemeral publish appends), so an uncapped route grows in Redis memory until
``maxmemory`` eviction takes the keyspace. 100k entries keeps the cap horizon at any plausible
emit rate far beyond the gateway's reclaim window; a total consumer outage longer than the
horizon loses the oldest undelivered signals — size to your recovery SLO, and alarm on
delivery lag long before the cap is the failure."""

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

    def spec_contributions(self) -> SpecRegistry:
        """Every spec this transport binds, for the application's inventory.

        Merge it at assembly (the same contract as ``AggregateKit.spec_contributions`` and
        ``forze_identity.spec_contributions``) — without it, reconciliation trips on the
        realtime routes as bound-but-never-catalogued. All three legs are operational
        planes (drained, not exported); the offline mailbox and cursor collections are
        ordinary ``DocumentSpec``s the app already declares via
        ``realtime_mailbox_spec``/``realtime_cursor_spec`` and registers itself.
        """

        return SpecRegistry().register(
            self.stream_spec,
            self.outbox_spec,
            self.inbox_spec,
            source=SpecSource.KIT,
        )


# ....................... #


def build_realtime_transport(channel: str = DEFAULT_REALTIME_CHANNEL) -> RealtimeTransport:
    """Build every realtime spec for *channel* from one name, so they stay in lock-step."""

    return RealtimeTransport(
        stream_spec=realtime_stream_spec(channel),
        outbox_spec=realtime_outbox_spec(channel, stream=channel),
        inbox_spec=realtime_inbox_spec(f"{channel}-inbox"),
    )
