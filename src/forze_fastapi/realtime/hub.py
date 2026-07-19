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
from typing import final
from uuid import UUID

import attrs

from forze.application.contracts.realtime import AudienceKind, RealtimeSignal
from forze.base.exceptions import exc

# ----------------------- #

__all__ = [
    "RealtimeSseHub",
    "SseSubscription",
]


@final
@attrs.define(slots=True, kw_only=True, eq=False)  # identity semantics: one per response
class SseSubscription:
    """One open SSE response's view of the hub: a bounded queue plus its match scope."""

    principal: str
    """The authenticated principal the stream belongs to."""

    tenant: UUID | None
    """The connection's bound tenant; a signal must carry the same tenant to match."""

    topics: frozenset[str] = frozenset()
    """Topic audiences this stream additionally receives (live-only, at-most-once)."""

    queue: asyncio.Queue[tuple[RealtimeSignal, str | None]] = attrs.field(init=False)
    """Matched signals with their durable event id (``None`` for ephemeral)."""

    maxsize: int = 256

    def __attrs_post_init__(self) -> None:
        self.queue = asyncio.Queue(maxsize=self.maxsize)

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


# ....................... #


@final
@attrs.define(slots=True)
class RealtimeSseHub:
    """Per-node broadcast hub between the tail loop and the open SSE responses."""

    queue_size: int = 256
    """Per-subscription queue bound; a full queue drops (at-most-once live leg)."""

    dropped: int = attrs.field(default=0, init=False)
    """Signals dropped on full subscriber queues since process start."""

    _subscriptions: set[SseSubscription] = attrs.field(factory=set, init=False)

    def __attrs_post_init__(self) -> None:
        if self.queue_size <= 0:
            raise exc.configuration("SSE hub queue_size must be positive")

    # ....................... #

    @property
    def subscribers(self) -> int:
        """How many SSE responses are currently subscribed on this node."""

        return len(self._subscriptions)

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

        return subscription

    # ....................... #

    def unsubscribe(self, subscription: SseSubscription) -> None:
        """Remove a subscription; idempotent (the response's ``finally`` may race a stop)."""

        self._subscriptions.discard(subscription)

    # ....................... #

    def publish(self, signal: RealtimeSignal, tenant: UUID | None, *, event_id: str | None) -> None:
        """Fan a signal out to every matching subscription (non-blocking, drop on full)."""

        for subscription in self._subscriptions:
            if not subscription.matches(signal, tenant):
                continue

            try:
                subscription.queue.put_nowait((signal, event_id))

            except asyncio.QueueFull:
                # At-most-once: a slow reader misses the live signal; durables reach it
                # via mailbox replay on its next reconnect.
                self.dropped += 1
