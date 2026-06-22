"""Offline store-and-forward mailbox — the seam + an in-memory implementation.

A durable signal addressed to a principal is **stored** (here) as well as emitted
live, and **replayed** to a device when it reconnects, so a recipient offline at
emit time still receives it. Two structural Protocols the gateway + connection
layer depend on (the app supplies the implementations, like ``RealtimePresence``):

- :class:`RealtimeMailbox` — the per-principal log: ``store`` / ``read_since`` / ``trim``.
- :class:`MailboxCursors` — per-device read positions: ``get`` / ``advance``.

The methods take an :class:`ExecutionContext` because a durable backing store
(the document store, RFC 0006 §4) is context-scoped (tenancy, transaction) — the
same way the gateway already calls ``ctx.inbox(spec)`` per signal. The in-memory
implementations here ignore it and are for tests / single-node development.

Ordering and the cursor value are an :class:`HlcTimestamp` — the HLC the durable
path already carries (``HEADER_HLC``), captured at store time.
"""

from ._compat import require_socketio

require_socketio()

# ....................... #

from typing import Awaitable, Protocol, final, runtime_checkable
from uuid import UUID

import attrs

from forze.application.contracts.realtime import MailboxEntry, RealtimeSignal
from forze.application.execution import ExecutionContext
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
        self,
        ctx: ExecutionContext,
        *,
        tenant: UUID | None,
        principal: str,
        event_id: str,
        hlc: HlcTimestamp,
        signal: RealtimeSignal,
    ) -> Awaitable[None]:
        """Append a durable signal (idempotent on ``event_id``)."""

        ...  # pragma: no cover

    def read_since(
        self,
        ctx: ExecutionContext,
        *,
        tenant: UUID | None,
        principal: str,
        since: HlcTimestamp | None,
    ) -> Awaitable[list[MailboxEntry]]:
        """The retained entries strictly after *since* (all when ``None``), oldest-first."""

        ...  # pragma: no cover

    def position_of(
        self,
        ctx: ExecutionContext,
        *,
        tenant: UUID | None,
        principal: str,
        event_id: str,
    ) -> Awaitable[HlcTimestamp | None]:
        """The HLC position of *event_id*, or ``None`` if no longer retained.

        The client acks by the event id it last saw (the frame ``id``); this maps it
        to the cursor position so :meth:`MailboxCursors.advance` can move the cursor."""

        ...  # pragma: no cover

    def trim(
        self,
        ctx: ExecutionContext,
        *,
        tenant: UUID | None,
        principal: str,
        before: HlcTimestamp,
    ) -> Awaitable[None]:
        """Drop entries at or before *before* (retention by age)."""

        ...  # pragma: no cover


# ....................... #


@runtime_checkable
class MailboxCursors(Protocol):
    """Per-device read positions over a principal's mailbox."""

    def get(
        self,
        ctx: ExecutionContext,
        *,
        tenant: UUID | None,
        principal: str,
        client_key: str,
    ) -> Awaitable[HlcTimestamp | None]:
        """The device's last-acked position, or ``None`` for a device seen first time."""

        ...  # pragma: no cover

    def advance(
        self,
        ctx: ExecutionContext,
        *,
        tenant: UUID | None,
        principal: str,
        client_key: str,
        up_to: HlcTimestamp,
    ) -> Awaitable[None]:
        """Advance the device's cursor to *up_to* (monotonic: never moves backwards)."""

        ...  # pragma: no cover


# ----------------------- #


def _key(tenant: UUID | None, principal: str) -> tuple[UUID | None, str]:
    return (tenant, principal)


@final
@attrs.define(slots=True)
class InMemoryRealtimeMailbox(RealtimeMailbox):
    """Single-node, in-memory mailbox. For multi-node use a durable store."""

    _logs: dict[tuple[UUID | None, str], list[MailboxEntry]] = attrs.field(
        factory=dict, init=False
    )

    async def store(
        self,
        ctx: ExecutionContext,
        *,
        tenant: UUID | None,
        principal: str,
        event_id: str,
        hlc: HlcTimestamp,
        signal: RealtimeSignal,
    ) -> None:
        log = self._logs.setdefault(_key(tenant, principal), [])

        if any(entry.event_id == event_id for entry in log):
            return  # idempotent on event_id

        log.append(
            MailboxEntry(
                event_id=event_id, hlc=hlc, event=signal.event, payload=dict(signal.payload)
            )
        )
        log.sort(key=lambda entry: entry.hlc)

    async def read_since(
        self,
        ctx: ExecutionContext,
        *,
        tenant: UUID | None,
        principal: str,
        since: HlcTimestamp | None,
    ) -> list[MailboxEntry]:
        log = self._logs.get(_key(tenant, principal), [])

        if since is None:
            return list(log)

        return [entry for entry in log if entry.hlc > since]

    async def position_of(
        self,
        ctx: ExecutionContext,
        *,
        tenant: UUID | None,
        principal: str,
        event_id: str,
    ) -> HlcTimestamp | None:
        for entry in self._logs.get(_key(tenant, principal), []):
            if entry.event_id == event_id:
                return entry.hlc

        return None

    async def trim(
        self,
        ctx: ExecutionContext,
        *,
        tenant: UUID | None,
        principal: str,
        before: HlcTimestamp,
    ) -> None:
        key = _key(tenant, principal)
        log = self._logs.get(key)

        if log is not None:
            self._logs[key] = [entry for entry in log if entry.hlc > before]


# ....................... #


@final
@attrs.define(slots=True)
class InMemoryMailboxCursors(MailboxCursors):
    """Single-node, in-memory per-device cursors."""

    _cursors: dict[tuple[UUID | None, str, str], HlcTimestamp] = attrs.field(
        factory=dict, init=False
    )

    async def get(
        self,
        ctx: ExecutionContext,
        *,
        tenant: UUID | None,
        principal: str,
        client_key: str,
    ) -> HlcTimestamp | None:
        return self._cursors.get((tenant, principal, client_key))

    async def advance(
        self,
        ctx: ExecutionContext,
        *,
        tenant: UUID | None,
        principal: str,
        client_key: str,
        up_to: HlcTimestamp,
    ) -> None:
        cursor_key = (tenant, principal, client_key)
        current = self._cursors.get(cursor_key)

        if current is None or up_to > current:  # monotonic: never moves backwards
            self._cursors[cursor_key] = up_to
