"""Offline realtime recipe — store-and-forward replayed on reconnect (mock, no Docker)."""

from __future__ import annotations

from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import DepsRegistry, ExecutionContext
from forze.base.primitives import HlcTimestamp
from forze_mock import MockDepsModule

from examples.recipes.realtime_offline.app import (
    TENANT,
    DocumentMailboxCursors,
    DocumentRealtimeMailbox,
    ack,
    emit_while_offline,
    reconnect,
    _signal,
)


def _ctx() -> ExecutionContext:
    return ExecutionContext(deps=DepsRegistry.from_modules(MockDepsModule()).freeze().resolve())


async def test_offline_signals_replay_then_ack_stops_re_replay() -> None:
    ctx = _ctx()
    mailbox = DocumentRealtimeMailbox()
    cursors = DocumentMailboxCursors()

    with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=TENANT)):
        await emit_while_offline(ctx, mailbox, event_id="e1", hlc=HlcTimestamp(physical_ms=1, logical=0), signal=_signal("shipped"))
        await emit_while_offline(ctx, mailbox, event_id="e2", hlc=HlcTimestamp(physical_ms=2, logical=0), signal=_signal("delivered"))

        # reconnect delivers both, in order
        first = await reconnect(ctx, mailbox, cursors, device="phone")
        assert [e.event_id for e in first] == ["e1", "e2"]
        assert [e.payload["text"] for e in first] == ["shipped", "delivered"]

        # ack the last → a later reconnect of the same device gets nothing
        await ack(ctx, mailbox, cursors, device="phone", event_id="e2")
        second = await reconnect(ctx, mailbox, cursors, device="phone")
        assert second == []
