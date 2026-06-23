"""Redis-backed, TTL-expiring realtime presence — crash-safe across nodes.

Structurally satisfies the ``RealtimePresence`` protocol that
:mod:`forze_socketio` consumes (``joined`` / ``left`` / ``count``); it is not
imported here so ``forze_redis`` keeps no dependency on the socket.io edge.

Where the in-memory tracker leaks a crashed node's rows forever, this stores
each room as a Redis sorted set scored by absolute expiry (server ``TIME`` +
TTL). A clean disconnect removes the member; a crash relies on the TTL, so
**live connections must heartbeat** (re-call ``joined`` within the TTL) — wire
``realtime_presence_heartbeat_lifecycle_step`` for that. Counts prune lapsed
members server-side, so a dead node's entries never inflate the tally.
"""

from forze_redis._compat import require_redis

require_redis()

# ....................... #

from datetime import timedelta
from typing import Final, final

import attrs

from forze.base.exceptions import exc

from ..kernel.client import RedisClientPort
from ..kernel.scripts import PRESENCE_COUNT, PRESENCE_JOIN, PRESENCE_LEAVE

# ----------------------- #

_DEFAULT_NAMESPACE: Final[str] = "forze"
_DEFAULT_TTL: Final[timedelta] = timedelta(seconds=90)


@final
@attrs.define(slots=True, kw_only=True)
class RedisRealtimePresence:
    """Distributed presence store with per-member TTL (crash-safe).

    The TTL must exceed the heartbeat interval so a live connection stays counted
    between refreshes; size it at a small multiple of the heartbeat.
    """

    client: RedisClientPort
    """Redis client used for the presence sorted sets."""

    namespace: str = _DEFAULT_NAMESPACE
    """Key namespace prefix (rooms already carry their tenant scope)."""

    ttl: timedelta = _DEFAULT_TTL
    """How long a member stays live without a heartbeat refresh."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        # A non-positive TTL would score every member as already-expired (and < 1ms truncates
        # to 0 ms), silently suppressing all presence tracking — fail closed at wiring instead.
        if self.ttl.total_seconds() < 0.001:
            raise exc.configuration("RedisRealtimePresence ttl must be at least 1 millisecond")

    # ....................... #

    def _key(self, room: str) -> str:
        return f"{self.namespace}:realtime:presence:{room}"

    # ....................... #

    async def joined(self, room: str, sid: str) -> None:
        await self.client.run_script(
            PRESENCE_JOIN,
            [self._key(room)],
            [sid, int(self.ttl.total_seconds() * 1000)],
        )

    # ....................... #

    async def left(self, room: str, sid: str) -> None:
        await self.client.run_script(PRESENCE_LEAVE, [self._key(room)], [sid])

    # ....................... #

    async def count(self, room: str) -> int:
        raw = await self.client.run_script(PRESENCE_COUNT, [self._key(room)], [])

        return int(raw)
