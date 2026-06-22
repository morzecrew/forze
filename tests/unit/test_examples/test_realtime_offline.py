"""Offline realtime recipe — store-and-forward replayed on reconnect (mock, no Docker)."""

from __future__ import annotations

from uuid import UUID

from forze.application.contracts.tenancy import TenantIdentity
from forze.base.primitives import HlcTimestamp
from forze_kits.integrations.realtime import build_realtime_cursors, build_realtime_mailbox

from examples.recipes.realtime_offline.app import (
    TENANT,
    _context,
    _signal,
    ack,
    emit_while_offline,
    reconnect,
)

_E1 = str(UUID(int=1))
_E2 = str(UUID(int=2))


async def test_offline_signals_replay_then_ack_stops_re_replay() -> None:
    ctx = _context()

    with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=TENANT)):
        mailbox = build_realtime_mailbox(ctx)
        cursors = build_realtime_cursors(ctx)

        await emit_while_offline(mailbox, event_id=_E1, hlc=HlcTimestamp(physical_ms=1, logical=0), signal=_signal("shipped"))
        await emit_while_offline(mailbox, event_id=_E2, hlc=HlcTimestamp(physical_ms=2, logical=0), signal=_signal("delivered"))

        # reconnect delivers both, in order
        first = await reconnect(mailbox, cursors, device="phone")
        assert [e.event_id for e in first] == [_E1, _E2]
        assert [e.payload["text"] for e in first] == ["shipped", "delivered"]

        # ack the last → a later reconnect of the same device gets nothing
        await ack(mailbox, cursors, device="phone", event_id=_E2)
        second = await reconnect(mailbox, cursors, device="phone")
        assert second == []
