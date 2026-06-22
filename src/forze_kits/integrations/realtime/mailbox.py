"""Document-backed offline mailbox + per-device cursors (the default implementations).

These structurally satisfy the ``RealtimeMailbox`` / ``MailboxCursors`` Protocols
that ``forze_socketio`` defines — they are **not** imported from there (so the kit
keeps no dependency on the socket.io edge), only the shared
:class:`~forze.application.contracts.realtime.MailboxEntry` data VO from core.

Both are tenant-global document collections keyed by explicit ``(tenant_id,
principal[, client_key])`` fields; the per-message tenant is bound into the
context for the write/read (the inbox-consumer pattern), so a tenant-partitioned
adapter scopes correctly and an unpartitioned one is isolated by the explicit
filter. Ordering and cursors use the HLC the durable path already carries
(``HEADER_HLC``), stored as its lexsortable encoding so range scans work.

Retention is by TTL/cap via :meth:`DocumentRealtimeMailbox.trim`; the document
store has no native TTL (RFC 0006 §6). Encryption is whatever the app configures
on the spec (no forced default).
"""

from contextlib import contextmanager
from typing import Any, Final, Iterator, final
from uuid import UUID, uuid5

import attrs
from pydantic import Field

from forze.application.contracts.document import DocumentSpec
from forze.application.contracts.realtime import MailboxEntry, RealtimeSignal
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import ExecutionContext
from forze.base.primitives import HlcTimestamp
from forze.domain.models import BaseDTO, Document, ReadDocument

from .specs import DEFAULT_REALTIME_CHANNEL

# ----------------------- #

_MAILBOX_NS: Final = UUID("6d61696c-626f-7800-0000-000000000000")
"""Fixed namespace for deriving deterministic mailbox/cursor document ids (uuid5)."""

_DEFAULT_CAP: Final = 1000
"""Max entries replayed per principal (newest-first retention bound)."""


# ----------------------- #
# document models


class _MailboxDoc(Document):
    tenant_id: UUID | None = None
    principal: str
    event_id: str
    hlc: int  # packed HlcTimestamp (monotonic int; range-queryable)
    event: str
    payload: dict[str, Any] = Field(default_factory=dict)


class _MailboxCreate(BaseDTO):
    tenant_id: UUID | None = None
    principal: str
    event_id: str
    hlc: int
    event: str
    payload: dict[str, Any] = Field(default_factory=dict)


class _MailboxRead(ReadDocument):
    tenant_id: UUID | None = None
    principal: str
    event_id: str
    hlc: int
    event: str
    payload: dict[str, Any] = Field(default_factory=dict)


class _CursorDoc(Document):
    tenant_id: UUID | None = None
    principal: str
    client_key: str
    hlc: int


class _CursorCreate(BaseDTO):
    tenant_id: UUID | None = None
    principal: str
    client_key: str
    hlc: int


class _CursorUpdate(BaseDTO):
    hlc: int


class _CursorRead(ReadDocument):
    tenant_id: UUID | None = None
    principal: str
    client_key: str
    hlc: int


# ----------------------- #
# specs


def realtime_mailbox_spec(
    channel: str = DEFAULT_REALTIME_CHANNEL,
) -> DocumentSpec[_MailboxRead, _MailboxDoc, _MailboxCreate, Any]:
    """The document collection holding per-principal durable signals."""

    return DocumentSpec(
        name=f"{channel}-mailbox",
        read=_MailboxRead,
        write={"domain": _MailboxDoc, "create_cmd": _MailboxCreate},
    )


def realtime_cursor_spec(
    channel: str = DEFAULT_REALTIME_CHANNEL,
) -> DocumentSpec[_CursorRead, _CursorDoc, _CursorCreate, _CursorUpdate]:
    """The document collection holding per-device read cursors."""

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


@contextmanager
def _bind_tenant(ctx: ExecutionContext, tenant: UUID | None) -> Iterator[None]:
    """Bind *tenant* for the enclosed document op (no-op when untenanted)."""

    if tenant is None:
        yield
        return

    with ctx.inv_ctx.bind_identity(
        authn=ctx.inv_ctx.get_authn(), tenant=TenantIdentity(tenant_id=tenant)
    ):
        yield


def _tenant_filter(tenant: UUID | None) -> Any:
    return {"$null": True} if tenant is None else tenant


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class DocumentRealtimeMailbox:
    """The offline mailbox over a document collection (RFC 0006 default)."""

    spec: DocumentSpec[_MailboxRead, _MailboxDoc, _MailboxCreate, Any] = attrs.field(
        factory=realtime_mailbox_spec
    )
    cap: int = _DEFAULT_CAP

    # ....................... #

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
        with _bind_tenant(ctx, tenant):
            await ctx.document.command(self.spec).ensure(
                uuid5(_MAILBOX_NS, f"{tenant}:{event_id}"),
                _MailboxCreate(
                    tenant_id=tenant,
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
        self,
        ctx: ExecutionContext,
        *,
        tenant: UUID | None,
        principal: str,
        since: HlcTimestamp | None,
    ) -> list[MailboxEntry]:
        values: dict[str, Any] = {"tenant_id": _tenant_filter(tenant), "principal": principal}

        if since is not None:
            values["hlc"] = {"$gt": since.pack()}

        with _bind_tenant(ctx, tenant):
            page = await ctx.document.query(self.spec).find_many(
                filters={"$values": values},
                sorts={"hlc": "asc"},
                pagination={"limit": self.cap},
            )

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
        self,
        ctx: ExecutionContext,
        *,
        tenant: UUID | None,
        principal: str,
        event_id: str,
    ) -> HlcTimestamp | None:
        with _bind_tenant(ctx, tenant):
            row = await ctx.document.query(self.spec).find(
                filters={
                    "$values": {
                        "tenant_id": _tenant_filter(tenant),
                        "principal": principal,
                        "event_id": event_id,
                    }
                }
            )

        return HlcTimestamp.unpack(row.hlc) if row is not None else None

    # ....................... #

    async def trim(
        self,
        ctx: ExecutionContext,
        *,
        tenant: UUID | None,
        principal: str,
        before: HlcTimestamp,
    ) -> None:
        values = {
            "tenant_id": _tenant_filter(tenant),
            "principal": principal,
            "hlc": {"$lte": before.pack()},
        }

        with _bind_tenant(ctx, tenant):
            query = ctx.document.query(self.spec)
            stale = await query.find_many(
                filters={"$values": values}, pagination={"limit": self.cap}
            )

            if stale.hits:
                await ctx.document.command(self.spec).kill_many(
                    [row.id for row in stale.hits]
                )


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class DocumentMailboxCursors:
    """Per-device read cursors over a document collection (RFC 0006 default)."""

    spec: DocumentSpec[_CursorRead, _CursorDoc, _CursorCreate, _CursorUpdate] = (
        attrs.field(factory=realtime_cursor_spec)
    )

    # ....................... #

    async def get(
        self,
        ctx: ExecutionContext,
        *,
        tenant: UUID | None,
        principal: str,
        client_key: str,
    ) -> HlcTimestamp | None:
        with _bind_tenant(ctx, tenant):
            row = await ctx.document.query(self.spec).find(
                filters={
                    "$values": {
                        "tenant_id": _tenant_filter(tenant),
                        "principal": principal,
                        "client_key": client_key,
                    }
                }
            )

        return HlcTimestamp.unpack(row.hlc) if row is not None else None

    # ....................... #

    async def advance(
        self,
        ctx: ExecutionContext,
        *,
        tenant: UUID | None,
        principal: str,
        client_key: str,
        up_to: HlcTimestamp,
    ) -> None:
        current = await self.get(ctx, tenant=tenant, principal=principal, client_key=client_key)

        if current is not None and up_to <= current:
            return  # monotonic: never moves backwards

        with _bind_tenant(ctx, tenant):
            await ctx.document.command(self.spec).upsert(
                uuid5(_MAILBOX_NS, f"cursor:{tenant}:{principal}:{client_key}"),
                _CursorCreate(
                    tenant_id=tenant,
                    principal=principal,
                    client_key=client_key,
                    hlc=up_to.pack(),
                ),
                _CursorUpdate(hlc=up_to.pack()),
                return_new=False,
            )
