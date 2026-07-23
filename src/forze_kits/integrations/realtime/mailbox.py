"""Document-backed offline mailbox + per-device cursors (the default implementations).

These structurally satisfy the ``RealtimeMailbox`` / ``MailboxCursors`` Protocols that
``forze_socketio`` defines — not imported here (so the kit keeps no dependency on the
socket.io edge), only the shared ``MailboxEntry`` / ``RealtimeSignal`` VOs from core.

**Dependencies are materialized at build** — ``build_realtime_mailbox`` /
``build_realtime_cursors`` resolve the document ports once (the publisher pattern), so a
misrouted spec fails at wiring, not on first emit, and a write-side build is refused in a
read-only (QUERY) operation. **Tenancy is the document store's concern** — wire the
mailbox/cursor collections ``tenant_aware`` and the adapter scopes every row by the
ambient tenant; the kit's one tenant touchpoint is the cursor id derivation, which must
share the store's notion of uniqueness. The mailbox doc's key is the durable event's own
id (already a ``UUID``); a cursor's key is a **deterministic** id derived from
``(tenant, principal, client_key)`` (``uuid5``), so concurrent first-acks converge on
one row and one principal's cursors never collide across tenants. Ordering/cursor values are the HLC the durable path carries, stored
packed (monotonic int, range-queryable). Encryption is whatever the app sets on the spec —
``realtime_mailbox_spec(encryption=...)`` seals the stored signal bodies at rest.
"""

from collections.abc import AsyncIterator
from datetime import datetime
from typing import Any, Final, final
from uuid import UUID, uuid5

import attrs
from pydantic import Field

from forze.application.contracts.crypto import FieldEncryption
from forze.application.contracts.document import (
    DocumentCommandPort,
    DocumentQueryPort,
    DocumentSpec,
)
from forze.application.contracts.querying import QueryFilterExpression
from forze.application.contracts.realtime import MailboxEntry, RealtimeSignal
from forze.application.contracts.tenancy import TenantIdentity, TenantProviderPort
from forze.application.execution import ExecutionContext
from forze.base.exceptions import CoreException, ExceptionKind, exc
from forze.base.primitives import HlcTimestamp
from forze.domain.models import BaseDTO, Document, ReadDocument
from forze_kits.integrations._logger import logger

from .specs import DEFAULT_REALTIME_CHANNEL

# ----------------------- #

_DEFAULT_CAP: Final = 1000
"""Max entries replayed per principal (newest-first retention bound)."""

_DEFAULT_REPLAY_PAGE_SIZE: Final = 100
"""Per-query page size for streamed replay (kept well below ``_DEFAULT_CAP``)."""

_CURSOR_NS: Final = UUID("1d3e0b5a-7c9f-4e2a-8b6d-0a1c2e4f6a8b")
"""Fixed namespace for deriving a cursor's id from ``(principal, client_key)``."""

_PRUNE_PAGE_SIZE: Final = 500
"""Rows deleted per page by the stale-cursor sweep (id-only projection)."""

_MAX_ADVANCE_ATTEMPTS: Final = 8
"""Retry budget for the cursor compare-and-advance loop.

Legitimate contention (a concurrent first-ack winning the insert, a concurrent advance
bumping the rev) converges within a round or two; exhausting this budget means the row
is unreachable — e.g. a foreign row holding the derived id outside the current tenant's
scope — and continuing would spin unboundedly on the user-facing ack path."""

# ....................... #


def _cursor_id(tenant: TenantIdentity | None, principal: str, client_key: str) -> UUID:
    """A deterministic cursor id, so concurrent first-acks for one device converge on a
    single row (the losing insert reconciles via a monotonic update) instead of racing
    two inserts.

    The **tenant is part of the derivation**: on the tagged-tenancy table shape this kit
    recommends, every tenant's rows share one physical primary-key space while the
    lookup is tenant-scoped. A tenant-blind id collides for a principal present in two
    tenants (the org-switcher flow): ``_find`` misses — the other tenant's row is
    invisible — while the create hits the other tenant's row on the PK, an insert that
    can never succeed. The id and the lookup must agree on their notion of uniqueness.

    ``uuid5`` is a SHA-1 hash of its inputs — no clock or entropy — so it needs no
    ``base.primitives`` seam (used directly, like ``hashlib``) and is byte-identical
    under simulation.
    """

    tenant_part = str(tenant.tenant_id) if tenant is not None else ""

    return uuid5(_CURSOR_NS, f"{tenant_part}\x00{principal}\x00{client_key}")


# ....................... #


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

    overflowed: int = 0
    """Replays whose backlog exceeded the retention cap (oldest overflow skipped).

    Every increment is a device that fell more than ``cap`` entries behind and lost
    the oldest part of its backlog — a bounded, declared loss, but one to alarm on."""


# ....................... #
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


# ....................... #
# specs

MailboxDocumentSpec = DocumentSpec[_MailboxRead, _MailboxDoc, _MailboxCreate, Any]
"""The mailbox collection's spec type (the models themselves stay private)."""

CursorDocumentSpec = DocumentSpec[_CursorRead, _CursorDoc, _CursorCreate, _CursorUpdate]
"""The cursor collection's spec type (the models themselves stay private)."""


def realtime_mailbox_spec(
    channel: str = DEFAULT_REALTIME_CHANNEL,
    *,
    encryption: FieldEncryption | None = None,
) -> DocumentSpec[_MailboxRead, _MailboxDoc, _MailboxCreate, Any]:
    """The document collection holding per-principal durable signals (wire it tenant-aware).

    Mailbox entries persist the **signal bodies** — DM texts, notification contents —
    for the whole retention window, so an app that seals its other collections should
    seal this one too. The document models are private, so *encryption* is the seam:
    pass a :class:`~forze.application.contracts.crypto.FieldEncryption` over the stored
    field names — seal ``payload`` (the signal body; usually also ``event``, the event
    name) with randomized encryption. ``principal``, ``event_id`` and ``hlc`` are the
    replay/ack index — the mailbox filters and sorts on them, so they must stay
    plaintext (a policy sealing them is refused at build).
    """

    if encryption is not None and (
        forbidden := encryption.sealed & {"principal", "event_id", "hlc"}
    ):
        raise exc.configuration(
            f"realtime_mailbox_spec cannot seal {sorted(forbidden)}: the mailbox "
            "filters and sorts on principal/event_id/hlc for replay, ack resolution, "
            "and trimming — sealed, every replay and ack would fail at query time. "
            "Seal 'payload' (and optionally 'event') instead.",
            code="realtime_mailbox_sealed_index",
        )

    return DocumentSpec(
        name=f"{channel}-mailbox",
        read=_MailboxRead,
        write={"domain": _MailboxDoc, "create_cmd": _MailboxCreate},
        encryption=encryption,
    )


# ....................... #


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


# ....................... #


@final
@attrs.define(slots=True, kw_only=True)  # not frozen — holds mutable counters
class DocumentRealtimeMailbox:
    """The offline mailbox over a document collection.

    Built via :func:`build_realtime_mailbox`. The document key is the durable
    ``event_id`` (a ``UUID``); a redelivery hits the primary-key conflict and is
    skipped, so ``store`` is idempotent and the ``stored`` counter tracks real writes.
    """

    command: DocumentCommandPort[_MailboxRead, _MailboxDoc, _MailboxCreate, Any]
    """The document command port for storing mailbox entries."""

    query: DocumentQueryPort[_MailboxRead]
    """The document query port for reading mailbox entries."""

    cap: int = _DEFAULT_CAP
    """The max entries replayed per principal (newest-first retention bound)."""

    replay_page_size: int = _DEFAULT_REPLAY_PAGE_SIZE
    """Per-query page size for :meth:`replay_since` (keyset-paged, well below :attr:`cap`)."""

    # ....................... #

    _stored: int = attrs.field(default=0, init=False)
    _replayed: int = attrs.field(default=0, init=False)
    _trimmed: int = attrs.field(default=0, init=False)
    _overflowed: int = attrs.field(default=0, init=False)

    # ....................... #

    def stats(self) -> MailboxStats:
        return MailboxStats(
            stored=self._stored,
            replayed=self._replayed,
            trimmed=self._trimmed,
            overflowed=self._overflowed,
        )

    # ....................... #

    async def store(
        self,
        *,
        principal: str,
        event_id: str,
        hlc: HlcTimestamp,
        signal: RealtimeSignal,
    ) -> None:
        try:
            await self.command.create(
                _MailboxCreate(
                    principal=principal,
                    event_id=event_id,
                    hlc=hlc.pack(),
                    event=signal.event,
                    payload=dict(signal.payload),
                ),
                id=UUID(event_id),  # the durable event's own id keys the row
                return_new=False,
            )
        except CoreException as error:
            # A conflict means this event is already stored — a relay retry or a
            # cross-resource crash redelivery. Idempotent: skip without recounting.
            if error.kind is ExceptionKind.CONFLICT:
                return
            raise

        self._stored += 1  # count only a real insert, honouring the "written" contract

    # ....................... #

    async def _window_filters(
        self, *, principal: str, since: HlcTimestamp | None
    ) -> QueryFilterExpression:
        """The filter selecting a **complete** replay window past *since*.

        The cap is a *newest-first retention bound*, so a backlog larger than the cap
        must lose its **oldest** entries, not its newest — and the loss must move the
        window start, never silently truncate the read. A truncated oldest-first read
        would deliver an incomplete prefix while later frames still flow live; the
        client's cumulative ack (which asserts "I have everything up to here") would
        then advance the cursor over entries that were never delivered, and the trim
        floor would delete them. Skipping *ahead* keeps the delivered prefix complete:
        everything at or after the window start is replayed, everything before it is a
        declared, counted retention loss.

        The window start is the **composite** ``(hlc, id)`` of the cap-th newest row —
        an HLC-only floor would match a whole equal-HLC run (the wall-clock fallback
        stamps a burst with one HLC), widening the window past the cap so the
        cap-limited ascending read delivers the run's *older* entries and drops the
        newest — the exact inversion of the retention bound.
        """

        values: dict[str, Any] = {"principal": principal}

        if since is not None:
            values["hlc"] = {"$gt": since.pack()}

        # One (hlc, id)-only probe, newest-first, asking for one row PAST the cap: a
        # backlog of exactly ``cap`` entries fits (a cap-limited probe could not tell
        # it from a real overflow and would count a false loss). More than ``cap``
        # rows means a true overflow, and the cap-th newest row is the window start.
        probe = await self.query.project_many(
            ["hlc", "id"],
            filters={"$values": values},
            sorts={"hlc": "desc", "id": "desc"},
            pagination={"limit": self.cap + 1},
        )

        if len(probe.hits) <= self.cap:
            return {"$values": values}  # the whole backlog fits — replay it all

        floor_row = probe.hits[self.cap - 1]
        floor_hlc = int(floor_row["hlc"])
        floor_id = UUID(str(floor_row["id"]))
        self._overflowed += 1
        logger.warning(
            "Realtime mailbox replay overflowed the retention cap; the oldest backlog was skipped",
            principal=principal,
            cap=self.cap,
            window_floor=str(HlcTimestamp.unpack(floor_hlc)),
        )

        # No `> since` term needed: every probed row already satisfies it, so the
        # floor bound subsumes it. The floor row itself is included ($gte on id).
        return {
            "$or": [
                {"$values": {"principal": principal, "hlc": {"$gt": floor_hlc}}},
                {"$values": {"principal": principal, "hlc": floor_hlc, "id": {"$gte": floor_id}}},
            ]
        }

    # ....................... #

    async def read_since(self, *, principal: str, since: HlcTimestamp | None) -> list[MailboxEntry]:
        page = await self.query.find_many(
            filters=await self._window_filters(principal=principal, since=since),
            sorts={"hlc": "asc", "id": "asc"},
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

    async def replay_since(
        self, *, principal: str, since: HlcTimestamp | None
    ) -> AsyncIterator[MailboxEntry]:
        """Stream entries after *since*, keyset-paged by ``(hlc, id)``, bounded by
        :attr:`cap`.

        The HLC is the per-principal position (the cursor value) but it is not unique —
        the wall-clock fallback stamps a whole burst with one HLC — so the keyset pages
        by the **composite** ``(hlc, id)``: an ``hlc > last`` keyset alone would
        permanently skip the rest of an equal-HLC run whenever a page boundary lands
        inside it. Only one page of rows is materialized at a time, so peak memory is
        one page per reconnecting device instead of the whole (up to :attr:`cap`)
        backlog. A backlog larger than the cap starts at the newest-``cap`` window
        (see :meth:`_window_filters`) — the stream is always a **complete** suffix of
        the retained backlog, never a truncated prefix.
        """

        filters: QueryFilterExpression = await self._window_filters(
            principal=principal, since=since
        )
        remaining = self.cap

        while remaining > 0:
            limit = min(self.replay_page_size, remaining)
            page = await self.query.find_many(
                filters=filters,
                sorts={"hlc": "asc", "id": "asc"},
                pagination={"limit": limit},
            )

            if not page.hits:
                return

            for row in page.hits:
                self._replayed += 1
                yield MailboxEntry(
                    event_id=row.event_id,
                    hlc=HlcTimestamp.unpack(row.hlc),
                    event=row.event,
                    payload=row.payload,
                )

            last = page.hits[-1]
            filters = {
                "$or": [
                    {"$values": {"principal": principal, "hlc": {"$gt": last.hlc}}},
                    {"$values": {"principal": principal, "hlc": last.hlc, "id": {"$gt": last.id}}},
                ]
            }
            remaining -= len(page.hits)

            # A short page means the backend has no more rows past the cursor.
            if len(page.hits) < limit:
                return

    # ....................... #

    async def position_of(self, *, principal: str, event_id: str) -> HlcTimestamp | None:
        row = await self.query.find(
            filters={"$values": {"principal": principal, "event_id": event_id}}
        )

        return HlcTimestamp.unpack(row.hlc) if row is not None else None

    # ....................... #

    async def trim(self, *, principal: str, before: HlcTimestamp) -> None:
        # Drain in pages until none remain — a single ``cap``-bounded page would leave stale
        # rows behind when more than ``cap`` have accumulated. Project only ``id``: the rows
        # are deleted, so hydrating each one's ``payload`` / ``event`` (potentially large
        # signal bodies) just to read its id is wasted memory and transfer.
        while True:
            stale = await self.query.project_many(
                ["id"],
                filters={"$values": {"principal": principal, "hlc": {"$lte": before.pack()}}},
                pagination={"limit": self.cap},
            )

            if not stale.hits:
                return

            await self.command.kill_many([UUID(str(row["id"])) for row in stale.hits])
            self._trimmed += len(stale.hits)

    # ....................... #

    async def sweep_older_than(self, *, cutoff: HlcTimestamp) -> int:
        """Delete entries older than *cutoff* across **every** principal; return how many.

        The age-based retention backstop the ack-driven :meth:`trim` cannot be: trimming
        follows the all-device cursor floor, and one stale device cursor (or a principal
        no device ever acks for) holds that floor forever — the ack path bounds *reads*
        (the replay cap), never storage. Run it from
        ``realtime_mailbox_retention_lifecycle_step``; an entry older than the retention
        window is deleted whether acked or not, a declared bound on how long offline
        delivery is owed.
        """

        deleted = 0

        while True:
            stale = await self.query.project_many(
                ["id"],
                filters={"$values": {"hlc": {"$lt": cutoff.pack()}}},
                pagination={"limit": self.cap},
            )

            if not stale.hits:
                return deleted

            await self.command.kill_many([UUID(str(row["id"])) for row in stale.hits])
            self._trimmed += len(stale.hits)
            deleted += len(stale.hits)


# ....................... #


@final
@attrs.define(slots=True, kw_only=True)  # not frozen — holds a mutable counter
class DocumentMailboxCursors:
    """Per-device read cursors over a document collection.

    Built via :func:`build_realtime_cursors`. A cursor is found by ``(principal, client_key)``
    (tenant-scoped by the store) and created under a **deterministic** id derived from
    ``(tenant, principal, client_key)`` (:func:`_cursor_id`), so concurrent first-acks
    converge on one row — the loser reconciles via a monotonic update rather than racing
    two inserts — and a principal present in two tenants never collides on the id.
    """

    command: DocumentCommandPort[_CursorRead, _CursorDoc, _CursorCreate, _CursorUpdate]
    """The document command port for creating and updating cursor rows."""

    query: DocumentQueryPort[_CursorRead]
    """The document query port for reading cursor rows."""

    tenant_provider: TenantProviderPort = lambda: None
    """Reads the ambient tenant at ack time — part of the cursor id derivation.

    The store scopes rows by tenant, so the deterministic id must too (see
    :func:`_cursor_id`); wired to ``ctx.inv_ctx.get_tenant`` by
    :func:`build_realtime_cursors`."""

    # ....................... #

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

    async def advance(self, *, principal: str, client_key: str, up_to: HlcTimestamp) -> None:
        target = up_to.pack()

        # Monotonic compare-and-advance. The first ack inserts under a deterministic
        # id; a concurrent first-ack that loses the insert hits a conflict and retries
        # via the update path — otherwise the loser's (possibly higher) position would
        # be silently dropped. The counter moves only after a write actually lands.
        # Bounded: legitimate races converge in a round or two, so exhausting the
        # budget means the row is unreachable and looping would pin the scope.
        for _ in range(_MAX_ADVANCE_ATTEMPTS):
            row = await self._find(principal, client_key)

            if row is None:
                try:
                    await self.command.create(
                        _CursorCreate(principal=principal, client_key=client_key, hlc=target),
                        id=_cursor_id(self.tenant_provider(), principal, client_key),
                        return_new=False,
                    )
                except CoreException as error:
                    if error.kind is ExceptionKind.CONFLICT:
                        continue  # a concurrent first-ack won the insert — reconcile
                    raise

                self._acked += 1
                return

            if target <= row.hlc:
                return  # monotonic: never moves backwards

            try:
                await self.command.update(
                    row.id, row.rev, _CursorUpdate(hlc=target), return_new=False
                )
            except CoreException as error:
                if error.kind is ExceptionKind.CONCURRENCY:
                    continue  # a concurrent advance bumped the rev — retry the CAS
                raise

            self._acked += 1
            return

        raise exc.internal(
            f"Realtime cursor advance for principal {principal!r} did not converge "
            f"after {_MAX_ADVANCE_ATTEMPTS} attempts: the cursor row is invisible to "
            "this scope yet its id conflicts on insert (a foreign row is holding the "
            "derived id — check the collection's tenancy wiring).",
            code="realtime_cursor_advance_stalled",
        )

    # ....................... #

    async def min_cursor(self, *, principal: str) -> HlcTimestamp | None:
        page = await self.query.find_many(
            filters={"$values": {"principal": principal}},
            sorts={"hlc": "asc"},
            pagination={"limit": 1},
        )

        return HlcTimestamp.unpack(page.hits[0].hlc) if page.hits else None

    # ....................... #

    async def prune_stale(self, *, idle_since: datetime) -> int:
        """Delete cursor rows that have not advanced since *idle_since*; return how many.

        Cursor rows are the device registry, and they otherwise never die: every
        per-connection fallback key (a Socket.IO ``sid``, a ``ws:`` uuid) mints a row on
        its first ack and then goes stale on disconnect — each one freezes the
        all-device trim floor at wherever it stopped, and the collection grows one row
        per connection forever. Staleness is the row's own ``last_update_at`` (bumped by
        every monotonic advance). A pruned row costs an active-but-quiet device nothing
        but one extra replay (a fresh cursor replays the retained window; the client
        dedups by envelope id) — run it from
        ``realtime_mailbox_retention_lifecycle_step`` with an idle window at least the
        mailbox's retention age.
        """

        pruned = 0

        while True:
            stale = await self.query.project_many(
                ["id"],
                filters={"$values": {"last_update_at": {"$lt": idle_since}}},
                pagination={"limit": _PRUNE_PAGE_SIZE},
            )

            if not stale.hits:
                return pruned

            await self.command.kill_many([UUID(str(row["id"])) for row in stale.hits])
            pruned += len(stale.hits)


# ----------------------- #
# build factories (resolve ports once; refuse a write-side build in read-only)


def build_realtime_mailbox(
    ctx: ExecutionContext,
    *,
    spec: DocumentSpec[_MailboxRead, _MailboxDoc, _MailboxCreate, Any] | None = None,
    cap: int = _DEFAULT_CAP,
    replay_page_size: int = _DEFAULT_REPLAY_PAGE_SIZE,
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
        replay_page_size=replay_page_size,
    )


def build_realtime_cursors(
    ctx: ExecutionContext,
    *,
    spec: (DocumentSpec[_CursorRead, _CursorDoc, _CursorCreate, _CursorUpdate] | None) = None,
) -> DocumentMailboxCursors:
    """Resolve the cursor collection's document ports once and build it (write-side guard)."""

    if ctx.inv_ctx.is_read_only():
        raise exc.precondition("Cannot build realtime cursors in a read-only (QUERY) operation")

    resolved = spec if spec is not None else realtime_cursor_spec()

    return DocumentMailboxCursors(
        command=ctx.document.command(resolved),
        query=ctx.document.query(resolved),
        tenant_provider=ctx.inv_ctx.get_tenant,
    )
