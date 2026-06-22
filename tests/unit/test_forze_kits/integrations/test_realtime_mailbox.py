"""Document-backed mailbox + cursors over the mock document store (M1)."""

from __future__ import annotations

from uuid import UUID

from forze.application.contracts.realtime import Audience, RealtimeSignal
from forze.application.execution import DepsRegistry, ExecutionRuntime
from forze.base.primitives import HlcTimestamp
from forze_kits.integrations.realtime import (
    DocumentMailboxCursors,
    DocumentRealtimeMailbox,
)
from forze_mock import MockDepsModule

# ----------------------- #

_T1 = UUID("11111111-1111-1111-1111-111111111111")
_T2 = UUID("22222222-2222-2222-2222-222222222222")


def _runtime() -> ExecutionRuntime:
    return ExecutionRuntime(deps=DepsRegistry.from_modules(MockDepsModule()).freeze())


def _hlc(physical_ms: int, logical: int = 0) -> HlcTimestamp:
    return HlcTimestamp(physical_ms=physical_ms, logical=logical)


def _signal(text: str) -> RealtimeSignal:
    return RealtimeSignal.of(Audience.principal("u1"), "order.shipped", {"text": text})


# ----------------------- #
# mailbox


async def test_store_and_read_since_ordered() -> None:
    mb = DocumentRealtimeMailbox()
    runtime = _runtime()

    async with runtime.scope():
        ctx = runtime.get_context()
        await mb.store(ctx, tenant=_T1, principal="u1", event_id="e2", hlc=_hlc(2), signal=_signal("b"))
        await mb.store(ctx, tenant=_T1, principal="u1", event_id="e1", hlc=_hlc(1), signal=_signal("a"))
        await mb.store(ctx, tenant=_T1, principal="u1", event_id="e3", hlc=_hlc(3), signal=_signal("c"))

        everything = await mb.read_since(ctx, tenant=_T1, principal="u1", since=None)
        after_e1 = await mb.read_since(ctx, tenant=_T1, principal="u1", since=_hlc(1))

    assert [e.event_id for e in everything] == ["e1", "e2", "e3"]
    assert everything[0].event == "order.shipped"
    assert everything[0].payload == {"text": "a"}
    assert [e.event_id for e in after_e1] == ["e2", "e3"]  # strictly after


async def test_store_is_idempotent_on_event_id() -> None:
    mb = DocumentRealtimeMailbox()
    runtime = _runtime()

    async with runtime.scope():
        ctx = runtime.get_context()
        await mb.store(ctx, tenant=_T1, principal="u1", event_id="e1", hlc=_hlc(1), signal=_signal("a"))
        await mb.store(ctx, tenant=_T1, principal="u1", event_id="e1", hlc=_hlc(1), signal=_signal("a"))

        rows = await mb.read_since(ctx, tenant=_T1, principal="u1", since=None)

    assert len(rows) == 1


async def test_mailbox_is_tenant_isolated() -> None:
    mb = DocumentRealtimeMailbox()
    runtime = _runtime()

    async with runtime.scope():
        ctx = runtime.get_context()
        await mb.store(ctx, tenant=_T1, principal="u1", event_id="e1", hlc=_hlc(1), signal=_signal("a"))

        same = await mb.read_since(ctx, tenant=_T1, principal="u1", since=None)
        other_tenant = await mb.read_since(ctx, tenant=_T2, principal="u1", since=None)
        other_principal = await mb.read_since(ctx, tenant=_T1, principal="u2", since=None)

    assert [e.event_id for e in same] == ["e1"]
    assert other_tenant == []
    assert other_principal == []


async def test_trim_drops_entries_at_or_before_cutoff() -> None:
    mb = DocumentRealtimeMailbox()
    runtime = _runtime()

    async with runtime.scope():
        ctx = runtime.get_context()
        for i in (1, 2, 3):
            await mb.store(ctx, tenant=_T1, principal="u1", event_id=f"e{i}", hlc=_hlc(i), signal=_signal(str(i)))

        await mb.trim(ctx, tenant=_T1, principal="u1", before=_hlc(2))
        rows = await mb.read_since(ctx, tenant=_T1, principal="u1", since=None)

    assert [e.event_id for e in rows] == ["e3"]


# ----------------------- #
# cursors


async def test_cursor_get_advance_is_monotonic() -> None:
    cursors = DocumentMailboxCursors()
    runtime = _runtime()

    async with runtime.scope():
        ctx = runtime.get_context()
        assert await cursors.get(ctx, tenant=_T1, principal="u1", client_key="d1") is None

        await cursors.advance(ctx, tenant=_T1, principal="u1", client_key="d1", up_to=_hlc(5))
        assert await cursors.get(ctx, tenant=_T1, principal="u1", client_key="d1") == _hlc(5)

        await cursors.advance(ctx, tenant=_T1, principal="u1", client_key="d1", up_to=_hlc(3))  # backwards
        assert await cursors.get(ctx, tenant=_T1, principal="u1", client_key="d1") == _hlc(5)

        await cursors.advance(ctx, tenant=_T1, principal="u1", client_key="d1", up_to=_hlc(8))
        assert await cursors.get(ctx, tenant=_T1, principal="u1", client_key="d1") == _hlc(8)


async def test_cursors_are_per_device_and_tenant() -> None:
    cursors = DocumentMailboxCursors()
    runtime = _runtime()

    async with runtime.scope():
        ctx = runtime.get_context()
        await cursors.advance(ctx, tenant=_T1, principal="u1", client_key="d1", up_to=_hlc(5))

        assert await cursors.get(ctx, tenant=_T1, principal="u1", client_key="d2") is None
        assert await cursors.get(ctx, tenant=_T2, principal="u1", client_key="d1") is None
        assert await cursors.get(ctx, tenant=_T1, principal="u1", client_key="d1") == _hlc(5)
