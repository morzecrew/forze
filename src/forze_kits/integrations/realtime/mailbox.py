"""Document-backed offline mailbox + per-device cursors (the default implementations).

These structurally satisfy the ``RealtimeMailbox`` / ``MailboxCursors`` Protocols that
``forze_socketio`` defines — not imported here (so the kit keeps no dependency on the
socket.io edge), only the shared ``MailboxEntry`` / ``RealtimeSignal`` VOs from core.

**Dependencies are materialized at build** — ``build_realtime_mailbox`` /
``build_realtime_cursors`` resolve the document ports once (the publisher pattern), so a
misrouted spec fails at wiring, not on first emit, and a write-side build is refused in a
read-only (QUERY) operation. **Tenancy is the document store's concern** — wire the
mailbox/cursor collections ``tenant_aware`` and the adapter scopes every row by the
ambient tenant; this kit carries **zero** tenant code. The mailbox doc's key is the
durable event's own id (already a ``UUID``); cursors look up by ``(principal, client_key)``
— no derived ids. Ordering/cursor values are the HLC the durable path carries, stored
packed (monotonic int, range-queryable). Encryption is whatever the app sets on the spec.
"""

from typing import Any, Final, final
from uuid import UUID

import attrs
from pydantic import Field

from forze.application.contracts.document import (
    DocumentCommandPort,
    DocumentQueryPort,
    DocumentSpec,
)
from forze.application.contracts.realtime import MailboxEntry, RealtimeSignal
from forze.application.execution import ExecutionContext
from forze.base.exceptions import exc
from forze.base.primitives import HlcTimestamp, uuid7
from forze.domain.models import BaseDTO, Document, ReadDocument

from .specs import DEFAULT_REALTIME_CHANNEL

# ----------------------- #

_DEFAULT_CAP: Final = 1000
"""Max entries replayed per principal (newest-first retention bound)."""


# ----------------------- #


@final
@attrs.define(slots=True, frozen=True)
class MailboxStats:
    """An immutable snapshot of a channel's offline-delivery counters.

    The implementations keep mutable counters internally and snapshot into this frozen
    value object on read — the :func:`~forze_kits.integrations.realtime.instrument_realtime_mailbox`
    pattern (mirrors the identity plane's ``SigningStats``)."""

    stored: int = 0
    """Durable principal signals written to the mailbox."""

    replayed: int = 0
    """Entries returned for connect-time replay."""

    trimmed: int = 0
    """Entries dropped by retention/ack trimming."""

    acked: int = 0
    """Cursor advances (device acks that moved a cursor forward)."""


# ----------------------- #
# document models (NO tenant_id — the tenant-aware adapter injects + scopes it)


class _MailboxDoc(Document):
    principal: str
    event_id: str
    hlc: int  # packed HlcTimestamp (monotonic int; range-queryable)
    event: str
    payload: dict[str, Any] = Field(default_factory=dict)


class _MailboxCreate(BaseDTO):
    principal: str
    event_id: str
    hlc: int
    event: str
    payload: dict[str, Any] = Field(default_factory=dict)


class _MailboxRead(ReadDocument):
    principal: str
    event_id: str
    hlc: int
    event: str
    payload: dict[str, Any] = Field(default_factory=dict)


class _CursorDoc(Document):
    principal: str
    client_key: str
    hlc: int


class _CursorCreate(BaseDTO):
    principal: str
    client_key: str
    hlc: int


class _CursorUpdate(BaseDTO):
    hlc: int


class _CursorRead(ReadDocument):
    principal: str
    client_key: str
    hlc: int


# ----------------------- #
# specs


def realtime_mailbox_spec(
    channel: str = DEFAULT_REALTIME_CHANNEL,
) -> DocumentSpec[_MailboxRead, _MailboxDoc, _MailboxCreate, Any]:
    """The document collection holding per-principal durable signals (wire it tenant-aware)."""

    return DocumentSpec(
        name=f"{channel}-mailbox",
        read=_MailboxRead,
        write={"domain": _MailboxDoc, "create_cmd": _MailboxCreate},
    )


def realtime_cursor_spec(
    channel: str = DEFAULT_REALTIME_CHANNEL,
) -> DocumentSpec[_CursorRead, _CursorDoc, _CursorCreate, _CursorUpdate]:
    """The document collection holding per-device read cursors (wire it tenant-aware)."""

    return DocumentSpec(
        name=f"{channel}-cursors",
        read=_CursorRead,
        write={
            "domain": _CursorDoc,
            "create_cmd": _CursorCreate,
            "update_cmd": _CursorUpdate,
        },
    )


# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True)  # not frozen — holds mutable counters
class DocumentRealtimeMailbox:
    """The offline mailbox over a document collection (RFC 0006 default).

    Built via :func:`build_realtime_mailbox`. The document key is the durable
    ``event_id`` (a ``UUID``), so ``ensure`` is idempotent on redelivery.
    """

    command: DocumentCommandPort[_MailboxRead, _MailboxDoc, _MailboxCreate, Any]
    query: DocumentQueryPort[_MailboxRead]
    cap: int = _DEFAULT_CAP

    _stored: int = attrs.field(default=0, init=False)
    _replayed: int = attrs.field(default=0, init=False)
    _trimmed: int = attrs.field(default=0, init=False)

    # ....................... #

    def stats(self) -> MailboxStats:
        return MailboxStats(stored=self._stored, replayed=self._replayed, trimmed=self._trimmed)

    # ....................... #

    async def store(
        self, *, principal: str, event_id: str, hlc: HlcTimestamp, signal: RealtimeSignal
    ) -> None:
        self._stored += 1

        await self.command.ensure(
            UUID(event_id),  # the durable event's own id — idempotent on redelivery
            _MailboxCreate(
                principal=principal,
                event_id=event_id,
                hlc=hlc.pack(),
                event=signal.event,
                payload=dict(signal.payload),
            ),
            return_new=False,
        )

    # ....................... #

    async def read_since(
        self, *, principal: str, since: HlcTimestamp | None
    ) -> list[MailboxEntry]:
        values: dict[str, Any] = {"principal": principal}

        if since is not None:
            values["hlc"] = {"$gt": since.pack()}

        page = await self.query.find_many(
            filters={"$values": values},
            sorts={"hlc": "asc"},
            pagination={"limit": self.cap},
        )

        self._replayed += len(page.hits)

        return [
            MailboxEntry(
                event_id=row.event_id,
                hlc=HlcTimestamp.unpack(row.hlc),
                event=row.event,
                payload=row.payload,
            )
            for row in page.hits
        ]

    # ....................... #

    async def position_of(
        self, *, principal: str, event_id: str
    ) -> HlcTimestamp | None:
        row = await self.query.find(
            filters={"$values": {"principal": principal, "event_id": event_id}}
        )

        return HlcTimestamp.unpack(row.hlc) if row is not None else None

    # ....................... #

    async def trim(self, *, principal: str, before: HlcTimestamp) -> None:
        stale = await self.query.find_many(
            filters={"$values": {"principal": principal, "hlc": {"$lte": before.pack()}}},
            pagination={"limit": self.cap},
        )

        if stale.hits:
            await self.command.kill_many([row.id for row in stale.hits])
            self._trimmed += len(stale.hits)


# ....................... #


@final
@attrs.define(slots=True, kw_only=True)  # not frozen — holds a mutable counter
class DocumentMailboxCursors:
    """Per-device read cursors over a document collection (RFC 0006 default).

    Built via :func:`build_realtime_cursors`. No derived ids — a cursor is found by
    ``(principal, client_key)`` and created with a fresh ``uuid7`` on first ack.
    """

    command: DocumentCommandPort[_CursorRead, _CursorDoc, _CursorCreate, _CursorUpdate]
    query: DocumentQueryPort[_CursorRead]

    _acked: int = attrs.field(default=0, init=False)

    # ....................... #

    def stats(self) -> MailboxStats:
        return MailboxStats(acked=self._acked)

    # ....................... #

    async def _find(self, principal: str, client_key: str) -> _CursorRead | None:
        return await self.query.find(
            filters={"$values": {"principal": principal, "client_key": client_key}}
        )

    # ....................... #

    async def get(self, *, principal: str, client_key: str) -> HlcTimestamp | None:
        row = await self._find(principal, client_key)

        return HlcTimestamp.unpack(row.hlc) if row is not None else None

    # ....................... #

    async def advance(
        self, *, principal: str, client_key: str, up_to: HlcTimestamp
    ) -> None:
        row = await self._find(principal, client_key)
        target = up_to.pack()

        if row is not None and target <= row.hlc:
            return  # monotonic: never moves backwards

        self._acked += 1

        if row is None:
            await self.command.create(
                _CursorCreate(principal=principal, client_key=client_key, hlc=target),
                id=uuid7(),
                return_new=False,
            )
        else:
            await self.command.update(row.id, row.rev, _CursorUpdate(hlc=target), return_new=False)

    # ....................... #

    async def min_cursor(self, *, principal: str) -> HlcTimestamp | None:
        page = await self.query.find_many(
            filters={"$values": {"principal": principal}},
            sorts={"hlc": "asc"},
            pagination={"limit": 1},
        )

        return HlcTimestamp.unpack(page.hits[0].hlc) if page.hits else None


# ----------------------- #
# build factories (resolve ports once; refuse a write-side build in read-only)


def build_realtime_mailbox(
    ctx: ExecutionContext,
    *,
    spec: DocumentSpec[_MailboxRead, _MailboxDoc, _MailboxCreate, Any] | None = None,
    cap: int = _DEFAULT_CAP,
) -> DocumentRealtimeMailbox:
    """Resolve the mailbox's document ports once and build it — the publisher pattern.

    Call from the gateway's ``run(ctx)`` or the connection layer's per-unit-of-work scope.
    Refuses a build in a read-only (QUERY) operation, since the mailbox writes.
    """

    if ctx.inv_ctx.is_read_only():
        raise exc.precondition("Cannot build a realtime mailbox in a read-only (QUERY) operation")

    resolved = spec if spec is not None else realtime_mailbox_spec()

    return DocumentRealtimeMailbox(
        command=ctx.document.command(resolved),
        query=ctx.document.query(resolved),
        cap=cap,
    )


def build_realtime_cursors(
    ctx: ExecutionContext,
    *,
    spec: DocumentSpec[_CursorRead, _CursorDoc, _CursorCreate, _CursorUpdate] | None = None,
) -> DocumentMailboxCursors:
    """Resolve the cursor collection's document ports once and build it (write-side guard)."""

    if ctx.inv_ctx.is_read_only():
        raise exc.precondition("Cannot build realtime cursors in a read-only (QUERY) operation")

    resolved = spec if spec is not None else realtime_cursor_spec()

    return DocumentMailboxCursors(
        command=ctx.document.command(resolved),
        query=ctx.document.query(resolved),
    )
