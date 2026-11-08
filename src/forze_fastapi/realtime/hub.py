"""The per-node SSE live-signal hub — broadcast fan-out to connected streams.

The consumer-group source is wrong for SSE's live leg: a group delivers each signal
to **one** consumer, but every node holding a matching SSE connection must see it.
The hub is the broadcast half: one per-node tail loop (see ``lifecycle``) publishes
every realtime signal into it, and each open SSE response holds a subscription that
receives the signals matching its principal/topics and tenant.

The live leg is at-most-once by contract — the mailbox carries the durable
guarantee — so a subscriber whose queue is full simply misses the signal (counted,
never blocking the tail loop or the other subscribers).
"""

from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

import asyncio
from collections.abc import Callable, Iterator, Sequence
from typing import Any, final
from uuid import UUID

import anyio
import attrs

from forze.application.contracts.realtime import Audience, AudienceKind, RealtimeSignal
from forze.application.integrations.realtime import RealtimePresence, room_for
from forze.base.exceptions import exc
from forze.base.logging import Logger
from forze.base.primitives import uuid7

# ----------------------- #

__all__ = [
    "HubDelivery",
    "RealtimeSseHub",
    "SseSubscription",
    "presence_rooms",
]


def presence_rooms(
    *, principal: str, tenant: UUID | None, topics: frozenset[str]
) -> tuple[str, ...]:
    """The presence rooms an SSE connection occupies — the same names Socket.IO uses.

    One naming scheme (:func:`~forze.application.integrations.realtime.room_for`)
    across transports, so "is this principal online" counts a Socket.IO connection
    and an open SSE stream identically.
    """

    return (
        room_for(Audience.principal(principal), tenant),
        *(room_for(Audience.topic(topic), tenant) for topic in sorted(topics)),
    )


_logger = Logger("fastapi.errors")

_PRESENCE_LEAVE_SECONDS = 2.0
"""The longest a closing connection spends leaving its presence rooms: far above a store's
round trips for a connection's few rooms, and short enough that a hung store cannot hold a
closing response — and with it a server's shutdown — for long. A member left behind
expires with the store's TTL."""


async def leave_presence_rooms(
    presence: RealtimePresence, rooms: Sequence[str], member: str, *, transport: str
) -> None:
    """Take *member* out of *rooms*, shielded from cancellation and bounded.

    A connection often closes cancelled — cut off by the drain, or its client gone — and
    a leave cancelled at its first round trip would keep the member counted until the
    store's TTL expires it. The shield holds against the cancel scopes Starlette and ASGI
    servers cancel through; a bare ``Task.cancel()`` still interrupts it. Best-effort: a
    failed leave is logged, and never masks the connection's own exit.

    :param transport: Names the connection in the logs, ``"SSE"`` or ``"WS"``.
    """

    with anyio.move_on_after(_PRESENCE_LEAVE_SECONDS, shield=True) as bound:
        for room in rooms:
            try:
                await presence.left(room, member)

            except Exception:
                _logger.exception("%s presence leave failed", transport, room=room)

    if bound.cancelled_caught:
        _logger.warning(
            "%s presence leave took over %ss; the rest of its rooms expire with the store's TTL",
            transport,
            _PRESENCE_LEAVE_SECONDS,
        )


@final
@attrs.define(slots=True, eq=False)
class HubDelivery:
    """One published signal, the same object in every matching subscription's queue.

    A transport's frame for it is a function of the signal and its event id alone, so
    :meth:`frame` encodes it once for all the streams it reaches rather than once per
    stream. Unpacks and indexes as the ``(signal, event_id)`` tuple it replaces.
    """

    signal: RealtimeSignal
    """The signal as published."""

    event_id: str | None
    """Its durable event id (``None`` for ephemeral)."""

    _frames: dict[Callable[[RealtimeSignal, str | None], str], str] = attrs.field(
        factory=dict, init=False, repr=False
    )

    def __iter__(self) -> Iterator[Any]:
        return iter((self.signal, self.event_id))

    def __len__(self) -> int:
        return 2

    def __getitem__(self, index: int) -> Any:
        # Indexes like the ``(signal, event_id)`` tuple the queues used to hold.
        return (self.signal, self.event_id)[index]

    # ....................... #

    def frame(self, encode: Callable[[RealtimeSignal, str | None], str]) -> str:
        """The frame *encode* makes of this delivery, encoded on the first call only.

        Keyed by *encode* alone, since this delivery fixes its other inputs: a hub that
        SSE and WebSocket share keeps one frame of each. Pass a module-level function —
        a fresh callable per call never hits.
        """

        frame = self._frames.get(encode)

        if frame is None:
            frame = self._frames[encode] = encode(self.signal, self.event_id)

        return frame


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, eq=False)  # identity semantics: one per response
class SseSubscription:
    """One open SSE response's view of the hub: a bounded queue plus its match scope.

    The hub indexes a subscription by its principal, tenant and topics as it subscribes,
    so none of them may change afterwards.
    """

    principal: str
    """The authenticated principal the stream belongs to."""

    tenant: UUID | None
    """The connection's bound tenant; a signal must carry the same tenant to match."""

    topics: frozenset[str] = frozenset()
    """Topic audiences this stream additionally receives (live-only, at-most-once)."""

    queue: asyncio.Queue[HubDelivery] = attrs.field(init=False)
    """Matched signals with their durable event id (``None`` for ephemeral)."""

    key: str = attrs.field(init=False)
    """This connection's presence member key (the SSE analog of the Socket.IO sid)."""

    ended: asyncio.Event = attrs.field(init=False)
    """Set by :meth:`end`: the stream should finish, so its client reconnects elsewhere.

    State rather than a sentinel in :attr:`queue`, which is bounded and drops when full."""

    maxsize: int = 256

    def __attrs_post_init__(self) -> None:
        self.queue = asyncio.Queue(maxsize=self.maxsize)
        self.key = f"sse:{uuid7()}"
        self.ended = asyncio.Event()

    # ....................... #

    def end(self) -> None:
        """Ask the stream to finish: an SSE response ends, a WebSocket closes with 1012."""

        self.ended.set()

    # ....................... #

    def rooms(self) -> tuple[str, ...]:
        """The presence rooms this subscription occupies (principal + topics)."""

        return presence_rooms(principal=self.principal, tenant=self.tenant, topics=self.topics)

    # ....................... #

    def matches(self, signal: RealtimeSignal, tenant: UUID | None) -> bool:
        """Whether *signal* (carried under *tenant*) addresses this subscription.

        Tenant equality is strict — an untenanted subscription never receives a
        tenanted signal and vice versa, mirroring the room scoping ``room_for``
        applies on the Socket.IO side.
        """

        if tenant != self.tenant:
            return False

        if signal.audience_kind is AudienceKind.PRINCIPAL:
            return signal.audience_name == self.principal

        return signal.audience_name in self.topics


def _audiences(subscription: SseSubscription) -> Iterator[tuple[AudienceKind, UUID | None, str]]:
    """The ``(kind, tenant, name)`` a signal must carry to match *subscription*, one per
    audience it receives — the hub's index keys, by the rules of :meth:`~SseSubscription.matches`."""

    yield AudienceKind.PRINCIPAL, subscription.tenant, subscription.principal

    for topic in subscription.topics:
        yield AudienceKind.TOPIC, subscription.tenant, topic


# ....................... #


@final
@attrs.define(slots=True)
class RealtimeSseHub:
    """Per-node broadcast hub between the tail loop and the open SSE responses."""

    queue_size: int = 256
    """Per-subscription queue bound; a full queue drops (at-most-once live leg)."""

    dropped: int = attrs.field(default=0, init=False)
    """Signals dropped on full subscriber queues since process start."""

    ready: asyncio.Event = attrs.field(init=False)
    """Set while live signals are flowing into the hub (initially set).

    The tail loop clears it for the duration of its startup fast-forward and sets it
    once live (and always on exit, so a dead tail never gates connects). The SSE
    route waits on it before resolving a connection's replay cursor: a durable the
    fast-forward window skipped is then already in the mailbox when the replay runs,
    instead of being missed until the client's next reconnect. A hub that is fed
    manually (tests, custom sources) is ready from construction.
    """

    _subscriptions: set[SseSubscription] = attrs.field(factory=set, init=False)

    _index: dict[tuple[AudienceKind, UUID | None, str], set[SseSubscription]] = attrs.field(
        factory=dict, init=False, repr=False
    )
    """Subscriptions by audience (see :func:`_audiences`): a publish reads one bucket."""

    _draining: Callable[[], bool] | None = attrs.field(default=None, init=False, repr=False)

    def __attrs_post_init__(self) -> None:
        if self.queue_size <= 0:
            raise exc.configuration("SSE hub queue_size must be positive")

        self.ready = asyncio.Event()
        self.ready.set()

    # ....................... #

    @property
    def draining(self) -> bool:
        """Whether the replica is draining, as the hub's drain step reports it.

        The SSE and WebSocket routes turn new streams away while it is true, whatever
        context their ``ctx_dep`` returns. ``False`` until a drain step follows the scope.
        """

        return self._draining is not None and self._draining()

    # ....................... #

    def follow_drain(self, draining: Callable[[], bool]) -> None:
        """Report *draining* as :attr:`draining`; the hub's drain step calls it at startup."""

        self._draining = draining

    # ....................... #

    @property
    def subscribers(self) -> int:
        """How many SSE responses are currently subscribed on this node."""

        return len(self._subscriptions)

    # ....................... #

    @property
    def subscriptions(self) -> tuple[SseSubscription, ...]:
        """A snapshot of the live subscriptions (for the presence heartbeat)."""

        return tuple(self._subscriptions)

    # ....................... #

    def subscribe(
        self,
        *,
        principal: str,
        tenant: UUID | None,
        topics: frozenset[str] = frozenset(),
    ) -> SseSubscription:
        """Register a subscription for one open SSE response. Pair with :meth:`unsubscribe`."""

        subscription = SseSubscription(
            principal=principal, tenant=tenant, topics=topics, maxsize=self.queue_size
        )
        self._subscriptions.add(subscription)

        for audience in _audiences(subscription):
            self._index.setdefault(audience, set()).add(subscription)

        return subscription

    # ....................... #

    def unsubscribe(self, subscription: SseSubscription) -> None:
        """Remove a subscription; idempotent (the response's ``finally`` may race a stop)."""

        if subscription not in self._subscriptions:
            return

        self._subscriptions.remove(subscription)

        for audience in _audiences(subscription):
            bucket = self._index[audience]
            bucket.discard(subscription)

            if not bucket:
                del self._index[audience]  # the index holds live audiences only

    # ....................... #

    def publish(self, signal: RealtimeSignal, tenant: UUID | None, *, event_id: str | None) -> None:
        """Fan a signal out to every matching subscription (non-blocking, drop on full)."""

        matching = self._index.get((signal.audience_kind, tenant, signal.audience_name))

        if not matching:
            return

        delivery = HubDelivery(signal, event_id)

        for subscription in matching:
            try:
                subscription.queue.put_nowait(delivery)

            except asyncio.QueueFull:
                # At-most-once: a slow reader misses the live signal; durables reach it
                # via mailbox replay on its next reconnect.
                self.dropped += 1
