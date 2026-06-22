"""Offline store-and-forward mailbox — the seam + an in-memory implementation.

A durable signal addressed to a principal is **stored** (here) as well as emitted
live, and **replayed** to a device when it reconnects, so a recipient offline at
emit time still receives it. Two structural Protocols the gateway + connection
layer depend on (the app supplies the implementations, like ``RealtimePresence``):

- :class:`RealtimeMailbox` — the per-principal log: ``store`` / ``read_since`` / ``trim``.
- :class:`MailboxCursors` — per-device read positions: ``get`` / ``advance``.

The methods take an :class:`ExecutionContext` because a durable backing store
(the document store, RFC 0006 §4) is context-scoped (tenancy, transaction) — the
same way the gateway already calls ``ctx.inbox(spec)`` per signal. **Tenant is
ambient**, never a parameter: the implementations read it from the bound context
(``ctx.inv_ctx.get_tenant()``), so a tenant-global worker (the gateway, the
connection layer) binds the per-signal / per-connection tenant before calling —
exactly as the publisher reads the ambient tenant for the message header.

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
    """A bounded, append-only per-principal log of recent durable signals.

    Scoped to the ambient tenant (read from the bound context); callers bind it."""

    def store(
        self,
        ctx: ExecutionContext,
        *,
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
        principal: str,
        since: HlcTimestamp | None,
    ) -> Awaitable[list[MailboxEntry]]:
        """The retained entries strictly after *since* (all when ``None``), oldest-first."""

        ...  # pragma: no cover

    def position_of(
        self,
        ctx: ExecutionContext,
        *,
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
        principal: str,
        before: HlcTimestamp,
    ) -> Awaitable[None]:
        """Drop entries at or before *before* (retention by age)."""

        ...  # pragma: no cover


# ....................... #


@runtime_checkable
class MailboxCursors(Protocol):
    """Per-device read positions over a principal's mailbox (ambient-tenant scoped)."""

    def get(
        self,
        ctx: ExecutionContext,
        *,
        principal: str,
        client_key: str,
    ) -> Awaitable[HlcTimestamp | None]:
        """The device's last-acked position, or ``None`` for a device seen first time."""

        ...  # pragma: no cover

    def advance(
        self,
        ctx: ExecutionContext,
        *,
        principal: str,
        client_key: str,
        up_to: HlcTimestamp,
    ) -> Awaitable[None]:
        """Advance the device's cursor to *up_to* (monotonic: never moves backwards)."""

        ...  # pragma: no cover

    def min_cursor(
        self,
        ctx: ExecutionContext,
        *,
        principal: str,
    ) -> Awaitable[HlcTimestamp | None]:
        """The lowest cursor across the principal's **known** devices, or ``None``.

        Entries at or before it have been acked by every device that has a cursor,
        so they can be trimmed. A device with no cursor row is not yet known (the
        cursor rows are the device registry); the TTL/cap is the backstop for it."""

        ...  # pragma: no cover


# ----------------------- #


def _tenant(ctx: ExecutionContext) -> UUID | None:
    """The ambient tenant id, the same source the publisher reads for the header."""

    tenant = ctx.inv_ctx.get_tenant()

    return tenant.tenant_id if tenant is not None else None


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
        principal: str,
        event_id: str,
        hlc: HlcTimestamp,
        signal: RealtimeSignal,
    ) -> None:
        log = self._logs.setdefault((_tenant(ctx), principal), [])

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
        principal: str,
        since: HlcTimestamp | None,
    ) -> list[MailboxEntry]:
        log = self._logs.get((_tenant(ctx), principal), [])

        if since is None:
            return list(log)

        return [entry for entry in log if entry.hlc > since]

    async def position_of(
        self,
        ctx: ExecutionContext,
        *,
        principal: str,
        event_id: str,
    ) -> HlcTimestamp | None:
        return next(
            (
                entry.hlc
                for entry in self._logs.get((_tenant(ctx), principal), [])
                if entry.event_id == event_id
            ),
            None,
        )

    async def trim(
        self,
        ctx: ExecutionContext,
        *,
        principal: str,
        before: HlcTimestamp,
    ) -> None:
        key = (_tenant(ctx), principal)
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
        principal: str,
        client_key: str,
    ) -> HlcTimestamp | None:
        return self._cursors.get((_tenant(ctx), principal, client_key))

    async def advance(
        self,
        ctx: ExecutionContext,
        *,
        principal: str,
        client_key: str,
        up_to: HlcTimestamp,
    ) -> None:
        cursor_key = (_tenant(ctx), principal, client_key)
        current = self._cursors.get(cursor_key)

        if current is None or up_to > current:  # monotonic: never moves backwards
            self._cursors[cursor_key] = up_to

    async def min_cursor(
        self,
        ctx: ExecutionContext,
        *,
        principal: str,
    ) -> HlcTimestamp | None:
        tenant = _tenant(ctx)
        positions = [
            hlc
            for (t, p, _key), hlc in self._cursors.items()
            if t == tenant and p == principal
        ]

        return min(positions, default=None)
