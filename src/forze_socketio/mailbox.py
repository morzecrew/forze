"""Offline store-and-forward mailbox — the seam + an in-memory implementation.

A durable signal addressed to a principal is **stored** (here) as well as emitted
live, and **replayed** to a device when it reconnects, so a recipient offline at
emit time still receives it. Two structural Protocols the gateway + connection
layer depend on (the app supplies the implementations, like ``RealtimePresence``):

- :class:`RealtimeMailbox` — the per-principal log: ``store`` / ``read_since`` / ``trim``.
- :class:`MailboxCursors` — per-device read positions: ``get`` / ``advance``.

The methods take **no context and no tenant** — tenancy and transactions are ambient
infrastructure. The document-backed default holds ports resolved once at build, which
scope by the bound tenant and join the current transaction on their own; a tenant-global
worker binds the tenant around the call. The in-memory implementation here is a
single-node test/dev aid keyed by ``principal`` only (multi-tenant isolation is the
durable store's concern).

Ordering and the cursor value are an :class:`HlcTimestamp` — the HLC the durable
path already carries (``HEADER_HLC``), captured at store time.
"""

from ._compat import require_socketio

require_socketio()

# ....................... #

from collections.abc import AsyncIterator, Awaitable
from typing import Protocol, final, runtime_checkable

import attrs

from forze.application.contracts.realtime import MailboxEntry, RealtimeSignal
from forze.base.exceptions import exc
from forze.base.primitives import HlcTimestamp

# ----------------------- #

__all__ = [
    "MailboxEntry",
    "RealtimeMailbox",
    "MailboxCursors",
    "InMemoryRealtimeMailbox",
    "InMemoryMailboxCursors",
]


@runtime_checkable
class RealtimeMailbox(Protocol):
    """A bounded, append-only per-principal log of recent durable signals."""

    def store(
        self, *, principal: str, event_id: str, hlc: HlcTimestamp, signal: RealtimeSignal
    ) -> Awaitable[None]:
        """Append a durable signal (idempotent on ``event_id``)."""

        ...  # pragma: no cover

    def read_since(
        self, *, principal: str, since: HlcTimestamp | None
    ) -> Awaitable[list[MailboxEntry]]:
        """The retained entries strictly after *since* (all when ``None``), oldest-first."""

        ...  # pragma: no cover

    def replay_since(
        self, *, principal: str, since: HlcTimestamp | None
    ) -> AsyncIterator[MailboxEntry]:
        """Stream the retained entries strictly after *since*, oldest-first.

        The streaming counterpart of :meth:`read_since`: the connection replay
        consumes it page-by-page (the document-backed store keyset-pages by HLC), so
        peak memory is one page rather than the whole retained backlog of every
        reconnecting device at once. Optional — the connection layer falls back to
        :meth:`read_since` for a mailbox that does not implement it.
        """

        ...  # pragma: no cover

    def position_of(self, *, principal: str, event_id: str) -> Awaitable[HlcTimestamp | None]:
        """The HLC position of *event_id*, or ``None`` if no longer retained.

        The client acks by the event id it last saw (the frame ``id``); this maps it
        to the cursor position so :meth:`MailboxCursors.advance` can move the cursor."""

        ...  # pragma: no cover

    def trim(self, *, principal: str, before: HlcTimestamp) -> Awaitable[None]:
        """Drop entries at or before *before* (retention by age/ack-floor)."""

        ...  # pragma: no cover


# ....................... #


@runtime_checkable
class MailboxCursors(Protocol):
    """Per-device read positions over a principal's mailbox."""

    def get(self, *, principal: str, client_key: str) -> Awaitable[HlcTimestamp | None]:
        """The device's last-acked position, or ``None`` for a device seen first time."""

        ...  # pragma: no cover

    def advance(self, *, principal: str, client_key: str, up_to: HlcTimestamp) -> Awaitable[None]:
        """Advance the device's cursor to *up_to* (monotonic: never moves backwards)."""

        ...  # pragma: no cover

    def min_cursor(self, *, principal: str) -> Awaitable[HlcTimestamp | None]:
        """The lowest cursor across the principal's **known** devices, or ``None``.

        Entries at or before it have been acked by every device that has a cursor,
        so they can be trimmed. A device with no cursor row is not yet known (the
        cursor rows are the device registry); the TTL/cap is the backstop for it."""

        ...  # pragma: no cover


# ----------------------- #


@final
@attrs.define(slots=True)
class InMemoryRealtimeMailbox(RealtimeMailbox):
    """Single-node, in-memory mailbox keyed by principal. For multi-node use a durable store."""

    cap: int = 1000
    """Per-principal retention cap (oldest evicted), matching the durable store's default.

    A dev/test aid must not grow without bound either — an uncapped in-memory mailbox
    wired into a long-lived process is a slow leak per never-acking principal.
    """

    _logs: dict[str, list[MailboxEntry]] = attrs.field(factory=dict, init=False)

    def __attrs_post_init__(self) -> None:
        if self.cap <= 0:
            # cap=0 would evict every entry on the store that added it — fail the wiring
            raise exc.configuration("Mailbox cap must be positive")

    async def store(
        self, *, principal: str, event_id: str, hlc: HlcTimestamp, signal: RealtimeSignal
    ) -> None:
        log = self._logs.setdefault(principal, [])

        if any(entry.event_id == event_id for entry in log):
            return  # idempotent on event_id

        log.append(
            MailboxEntry(
                event_id=event_id, hlc=hlc, event=signal.event, payload=dict(signal.payload)
            )
        )
        log.sort(key=lambda entry: entry.hlc)

        if len(log) > self.cap:
            del log[: len(log) - self.cap]

    async def read_since(self, *, principal: str, since: HlcTimestamp | None) -> list[MailboxEntry]:
        log = self._logs.get(principal, [])

        if since is None:
            return list(log)

        return [entry for entry in log if entry.hlc > since]

    async def replay_since(
        self, *, principal: str, since: HlcTimestamp | None
    ) -> AsyncIterator[MailboxEntry]:
        # Single-node test aid: the log is already in memory, so this just yields in
        # order — the paging win is the durable store's (see ``DocumentRealtimeMailbox``).
        for entry in await self.read_since(principal=principal, since=since):
            yield entry

    async def position_of(self, *, principal: str, event_id: str) -> HlcTimestamp | None:
        return next(
            (entry.hlc for entry in self._logs.get(principal, []) if entry.event_id == event_id),
            None,
        )

    async def trim(self, *, principal: str, before: HlcTimestamp) -> None:
        log = self._logs.get(principal)

        if log is not None:
            self._logs[principal] = [entry for entry in log if entry.hlc > before]


# ....................... #


@final
@attrs.define(slots=True)
class InMemoryMailboxCursors(MailboxCursors):
    """Single-node, in-memory per-device cursors keyed by ``(principal, client_key)``."""

    _cursors: dict[tuple[str, str], HlcTimestamp] = attrs.field(factory=dict, init=False)

    async def get(self, *, principal: str, client_key: str) -> HlcTimestamp | None:
        return self._cursors.get((principal, client_key))

    async def advance(self, *, principal: str, client_key: str, up_to: HlcTimestamp) -> None:
        key = (principal, client_key)
        current = self._cursors.get(key)

        if current is None or up_to > current:  # monotonic: never moves backwards
            self._cursors[key] = up_to

    async def min_cursor(self, *, principal: str) -> HlcTimestamp | None:
        positions = [hlc for (p, _key), hlc in self._cursors.items() if p == principal]

        return min(positions, default=None)
