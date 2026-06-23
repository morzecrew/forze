"""Recipe: offline realtime delivery — store-and-forward to a reconnecting device.

A durable, principal-addressed realtime signal sent while the recipient is offline
is stored in a per-recipient **mailbox** and replayed when their device reconnects;
a per-device **cursor** (advanced by the client's ack) means a device never
re-receives what it already saw. In a live app the Socket.IO gateway calls
``mailbox.store`` and ``attach_realtime_connection(mailbox_factory=…, cursors_factory=…)``
does the reconnect replay + ``realtime.ack`` — here we drive the same public components
in-process to show the semantics, no sockets required. Mock-runnable.

The mailbox and cursors are built with their document ports resolved
(``build_realtime_mailbox`` / ``build_realtime_cursors``), and the collections are
**tenant-aware**: the document store scopes every row by the ambient tenant, so this
code carries no tenant logic — a worker binds the tenant (here ``main`` binds it once).

Run it:  uv run python -m examples.recipes.realtime_offline.app
Exercised by tests/unit/test_examples/test_realtime_offline.py.
"""

from __future__ import annotations

import asyncio
from uuid import UUID

from forze.application.contracts.realtime import Audience, MailboxEntry, RealtimeSignal
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import DepsRegistry, ExecutionContext
from forze.base.primitives import HlcTimestamp, uuid7
from forze_kits.integrations.realtime import (
    DocumentMailboxCursors,
    DocumentRealtimeMailbox,
    build_realtime_cursors,
    build_realtime_mailbox,
    realtime_cursor_spec,
    realtime_mailbox_spec,
)
from forze_mock import MockDepsModule
from forze_mock.execution import MockRouteConfig

# --8<-- [start:setup]
TENANT = UUID("11111111-1111-1111-1111-111111111111")
BOB = "bob"  # the recipient principal (Audience.principal id form)


def _signal(text: str) -> RealtimeSignal:
    return RealtimeSignal.of(Audience.principal(BOB), "order.shipped", {"text": text})
# --8<-- [end:setup]


# --8<-- [start:emit]
async def emit_while_offline(
    mailbox: DocumentRealtimeMailbox,
    *,
    event_id: str,
    hlc: HlcTimestamp,
    signal: RealtimeSignal,
) -> None:
    """What the gateway does for a durable principal signal: store it for replay.

    (Online it would also ``sio.emit``; offline — no room members — the mailbox is the
    only delivery, drained on reconnect.) No tenant here — the store scopes ambiently.
    """

    await mailbox.store(principal=BOB, event_id=event_id, hlc=hlc, signal=signal)
# --8<-- [end:emit]


# --8<-- [start:reconnect]
async def reconnect(
    mailbox: DocumentRealtimeMailbox,
    cursors: DocumentMailboxCursors,
    *,
    device: str,
) -> list[MailboxEntry]:
    """What the connection layer replays on connect: everything past this device's cursor."""

    since = await cursors.get(principal=BOB, client_key=device)

    return await mailbox.read_since(principal=BOB, since=since)


async def ack(
    mailbox: DocumentRealtimeMailbox,
    cursors: DocumentMailboxCursors,
    *,
    device: str,
    event_id: str,
) -> None:
    """What ``realtime.ack {up_to}`` does: advance the device cursor, trim what all acked."""

    position = await mailbox.position_of(principal=BOB, event_id=event_id)

    if position is not None:
        await cursors.advance(principal=BOB, client_key=device, up_to=position)
        floor = await cursors.min_cursor(principal=BOB)

        if floor is not None:
            await mailbox.trim(principal=BOB, before=floor)
# --8<-- [end:reconnect]


def _context() -> ExecutionContext:
    # the mailbox + cursor collections are tenant-aware — the store scopes them
    routes = {
        str(realtime_mailbox_spec().name): MockRouteConfig(tenant_aware=True),
        str(realtime_cursor_spec().name): MockRouteConfig(tenant_aware=True),
    }
    return ExecutionContext(
        deps=DepsRegistry.from_modules(MockDepsModule(routes=routes)).freeze().resolve()
    )


async def main() -> None:
    ctx = _context()

    # A worker binds the recipient's tenant; the mailbox reads it ambiently via the store.
    with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=TENANT)):
        mailbox = build_realtime_mailbox(ctx)
        cursors = build_realtime_cursors(ctx)

        # Two durable signals arrive while Bob's phone is offline (ids are the durable
        # forze_event_id — UUIDs).
        e_shipped, e_delivered = str(uuid7()), str(uuid7())
        await emit_while_offline(mailbox, event_id=e_shipped, hlc=HlcTimestamp(physical_ms=1, logical=0), signal=_signal("shipped"))
        await emit_while_offline(mailbox, event_id=e_delivered, hlc=HlcTimestamp(physical_ms=2, logical=0), signal=_signal("delivered"))

        # Bob's phone reconnects → it receives both, in order.
        first = await reconnect(mailbox, cursors, device="phone")
        print(f"phone reconnect: {[e.payload['text'] for e in first]}")

        # The client acks the last one it processed.
        await ack(mailbox, cursors, device="phone", event_id=e_delivered)

        # A later reconnect of the same device replays nothing — it's caught up.
        second = await reconnect(mailbox, cursors, device="phone")
        print(f"phone reconnect again: {[e.payload['text'] for e in second]}")


if __name__ == "__main__":
    asyncio.run(main())
