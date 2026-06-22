"""Recipe: offline realtime delivery — store-and-forward to a reconnecting device.

A durable, principal-addressed realtime signal sent while the recipient is offline
is stored in a per-recipient **mailbox** and replayed when their device reconnects;
a per-device **cursor** (advanced by the client's ack) means a device never
re-receives what it already saw. In a live app the Socket.IO gateway calls
``mailbox.store`` (instead of, or alongside, ``sio.emit``) and
``attach_realtime_connection(mailbox=…, cursors=…)`` does the reconnect replay +
``realtime.ack`` — here we drive the same public components in-process to show the
semantics, no sockets required. Mock-runnable.

Run it:  uv run python -m examples.recipes.realtime_offline.app
Exercised by tests/unit/test_examples/test_realtime_offline.py.
"""

from __future__ import annotations

import asyncio
from uuid import UUID

from forze.application.contracts.realtime import Audience, MailboxEntry, RealtimeSignal
from forze.application.execution import DepsRegistry, ExecutionContext
from forze.base.primitives import HlcTimestamp
from forze_kits.integrations.realtime import (
    DocumentMailboxCursors,
    DocumentRealtimeMailbox,
)
from forze_mock import MockDepsModule

# --8<-- [start:setup]
TENANT = UUID("11111111-1111-1111-1111-111111111111")
BOB = "bob"  # the recipient principal (Audience.principal id form)


def _signal(text: str) -> RealtimeSignal:
    return RealtimeSignal.of(Audience.principal(BOB), "order.shipped", {"text": text})
# --8<-- [end:setup]


# --8<-- [start:emit]
async def emit_while_offline(
    ctx: ExecutionContext,
    mailbox: DocumentRealtimeMailbox,
    *,
    event_id: str,
    hlc: HlcTimestamp,
    signal: RealtimeSignal,
) -> None:
    """What the gateway does for a durable principal signal: store it for replay.

    (Online, it would also ``sio.emit``; offline — no room members — the mailbox is
    the only delivery, drained on reconnect.)
    """

    await mailbox.store(ctx, tenant=TENANT, principal=BOB, event_id=event_id, hlc=hlc, signal=signal)
# --8<-- [end:emit]


# --8<-- [start:reconnect]
async def reconnect(
    ctx: ExecutionContext,
    mailbox: DocumentRealtimeMailbox,
    cursors: DocumentMailboxCursors,
    *,
    device: str,
) -> list[MailboxEntry]:
    """What the connection layer replays on connect: everything past this device's cursor."""

    since = await cursors.get(ctx, tenant=TENANT, principal=BOB, client_key=device)

    return await mailbox.read_since(ctx, tenant=TENANT, principal=BOB, since=since)


async def ack(
    ctx: ExecutionContext,
    mailbox: DocumentRealtimeMailbox,
    cursors: DocumentMailboxCursors,
    *,
    device: str,
    event_id: str,
) -> None:
    """What ``realtime.ack {up_to}`` does: advance the device cursor, trim what all acked."""

    position = await mailbox.position_of(ctx, tenant=TENANT, principal=BOB, event_id=event_id)

    if position is not None:
        await cursors.advance(ctx, tenant=TENANT, principal=BOB, client_key=device, up_to=position)
        floor = await cursors.min_cursor(ctx, tenant=TENANT, principal=BOB)

        if floor is not None:
            await mailbox.trim(ctx, tenant=TENANT, principal=BOB, before=floor)
# --8<-- [end:reconnect]


async def main() -> None:
    ctx = ExecutionContext(
        deps=DepsRegistry.from_modules(MockDepsModule()).freeze().resolve()
    )
    mailbox = DocumentRealtimeMailbox()
    cursors = DocumentMailboxCursors()

    # Two durable signals arrive while Bob's phone is offline.
    await emit_while_offline(ctx, mailbox, event_id="e1", hlc=HlcTimestamp(physical_ms=1, logical=0), signal=_signal("shipped"))
    await emit_while_offline(ctx, mailbox, event_id="e2", hlc=HlcTimestamp(physical_ms=2, logical=0), signal=_signal("delivered"))

    # Bob's phone reconnects → it receives both, in order.
    first = await reconnect(ctx, mailbox, cursors, device="phone")
    print(f"phone reconnect: {[e.payload['text'] for e in first]}")

    # The client acks the last one it processed.
    await ack(ctx, mailbox, cursors, device="phone", event_id="e2")

    # A later reconnect of the same device replays nothing — it's caught up.
    second = await reconnect(ctx, mailbox, cursors, device="phone")
    print(f"phone reconnect again: {[e.payload['text'] for e in second]}")


if __name__ == "__main__":
    asyncio.run(main())
