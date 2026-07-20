"""Presence — who occupies an audience scope right now, across every transport.

Transport-neutral on purpose: a Socket.IO connection and an open SSE stream are
both "this principal can receive a live signal now", and any presence-based
decision (is the user online? fall back to email?) is only honest if every
transport reports into the **same** store under the same room names
(:func:`~forze.application.integrations.realtime.room_for`).

The in-memory tracker is a single-node aid; a cross-node deployment wants a
TTL-backed store (e.g. ``RedisRealtimePresence``) so a crashed node's entries
expire rather than leak — which in turn means live connections must heartbeat
(each transport ships a heartbeat lifecycle step).
"""

from collections.abc import Awaitable
from typing import ClassVar, Protocol, final, runtime_checkable

import attrs

# ----------------------- #

__all__ = [
    "RealtimePresence",
    "InMemoryRealtimePresence",
]


@runtime_checkable
class RealtimePresence(Protocol):
    """Tracks how many connections occupy a room (e.g. is a principal online).

    An implementation whose counts are visible to **every** node (a shared store like
    ``RedisRealtimePresence``) should declare ``cluster_wide: ClassVar[bool] = True``.
    Consumers that make skip-delivery decisions from a count (the gateway's
    presence-based emit skip) treat an absent or ``False`` marker as node-local and
    refuse to pair it with a multi-node Socket.IO backplane — a node-local count of 0
    there just means "not on *this* node", not "offline".
    """

    def joined(self, room: str, sid: str) -> Awaitable[None]: ...  # pragma: no cover

    def left(self, room: str, sid: str) -> Awaitable[None]: ...  # pragma: no cover

    def count(self, room: str) -> Awaitable[int]: ...  # pragma: no cover


# ....................... #


@final
@attrs.define(slots=True)
class InMemoryRealtimePresence(RealtimePresence):
    """Single-node, in-memory presence. For multi-node use a TTL-backed store."""

    cluster_wide: ClassVar[bool] = False
    """Counts cover this process only — never a basis for multi-node skip decisions."""

    _rooms: dict[str, set[str]] = attrs.field(factory=dict, init=False)

    async def joined(self, room: str, sid: str) -> None:
        self._rooms.setdefault(room, set()).add(sid)

    async def left(self, room: str, sid: str) -> None:
        members = self._rooms.get(room)

        if members is not None:
            members.discard(sid)

            if not members:  # drop the empty bucket so churn doesn't leak room keys
                del self._rooms[room]

    async def count(self, room: str) -> int:
        return len(self._rooms.get(room, ()))
