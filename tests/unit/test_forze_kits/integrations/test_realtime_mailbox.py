"""Document-backed mailbox + cursors over the mock document store, ambient tenant (M1)."""

from __future__ import annotations

from contextlib import AbstractContextManager
from uuid import UUID

from forze.application.contracts.realtime import Audience, RealtimeSignal
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import DepsRegistry, ExecutionContext, ExecutionRuntime
from forze.base.primitives import HlcTimestamp
from forze_kits.integrations.realtime import (
    DocumentMailboxCursors,
    DocumentRealtimeMailbox,
    MailboxStats,
)
from forze_mock import MockDepsModule

# ----------------------- #

_T1 = UUID("11111111-1111-1111-1111-111111111111")
_T2 = UUID("22222222-2222-2222-2222-222222222222")


def _runtime() -> ExecutionRuntime:
    return ExecutionRuntime(deps=DepsRegistry.from_modules(MockDepsModule()).freeze())


def _bind(ctx: ExecutionContext, tenant: UUID = _T1) -> AbstractContextManager[None]:
    """Tenant is ambient — the worker binds it; the mailbox reads it from the context."""

    return ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=tenant))


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
        with _bind(ctx):
            await mb.store(ctx, principal="u1", event_id="e2", hlc=_hlc(2), signal=_signal("b"))
            await mb.store(ctx, principal="u1", event_id="e1", hlc=_hlc(1), signal=_signal("a"))
            await mb.store(ctx, principal="u1", event_id="e3", hlc=_hlc(3), signal=_signal("c"))

            everything = await mb.read_since(ctx, principal="u1", since=None)
            after_e1 = await mb.read_since(ctx, principal="u1", since=_hlc(1))

    assert [e.event_id for e in everything] == ["e1", "e2", "e3"]
    assert everything[0].event == "order.shipped"
    assert everything[0].payload == {"text": "a"}
    assert [e.event_id for e in after_e1] == ["e2", "e3"]  # strictly after


async def test_store_is_idempotent_on_event_id() -> None:
    mb = DocumentRealtimeMailbox()
    runtime = _runtime()

    async with runtime.scope():
        ctx = runtime.get_context()
        with _bind(ctx):
            await mb.store(ctx, principal="u1", event_id="e1", hlc=_hlc(1), signal=_signal("a"))
            await mb.store(ctx, principal="u1", event_id="e1", hlc=_hlc(1), signal=_signal("a"))
            rows = await mb.read_since(ctx, principal="u1", since=None)

    assert len(rows) == 1


async def test_mailbox_is_tenant_isolated() -> None:
    mb = DocumentRealtimeMailbox()
    runtime = _runtime()

    async with runtime.scope():
        ctx = runtime.get_context()
        with _bind(ctx, _T1):
            await mb.store(ctx, principal="u1", event_id="e1", hlc=_hlc(1), signal=_signal("a"))
            same = await mb.read_since(ctx, principal="u1", since=None)
            other_principal = await mb.read_since(ctx, principal="u2", since=None)
        with _bind(ctx, _T2):
            other_tenant = await mb.read_since(ctx, principal="u1", since=None)

    assert [e.event_id for e in same] == ["e1"]
    assert other_tenant == []
    assert other_principal == []


async def test_trim_drops_entries_at_or_before_cutoff() -> None:
    mb = DocumentRealtimeMailbox()
    runtime = _runtime()

    async with runtime.scope():
        ctx = runtime.get_context()
        with _bind(ctx):
            for i in (1, 2, 3):
                await mb.store(ctx, principal="u1", event_id=f"e{i}", hlc=_hlc(i), signal=_signal(str(i)))

            await mb.trim(ctx, principal="u1", before=_hlc(2))
            rows = await mb.read_since(ctx, principal="u1", since=None)

    assert [e.event_id for e in rows] == ["e3"]


async def test_shared_stats_count_store_replay_trim_ack() -> None:
    stats = MailboxStats()
    mb = DocumentRealtimeMailbox(stats=stats)
    cursors = DocumentMailboxCursors(stats=stats)
    runtime = _runtime()

    async with runtime.scope():
        ctx = runtime.get_context()
        with _bind(ctx):
            await mb.store(ctx, principal="u1", event_id="e1", hlc=_hlc(1), signal=_signal("a"))
            await mb.store(ctx, principal="u1", event_id="e2", hlc=_hlc(2), signal=_signal("b"))
            replayed = await mb.read_since(ctx, principal="u1", since=None)
            await cursors.advance(ctx, principal="u1", client_key="d1", up_to=_hlc(2))
            await mb.trim(ctx, principal="u1", before=_hlc(1))

    assert len(replayed) == 2
    assert stats.stored == 2
    assert stats.replayed == 2
    assert stats.acked == 1
    assert stats.trimmed == 1  # e1 dropped


# ----------------------- #
# cursors


async def test_cursor_get_advance_is_monotonic() -> None:
    cursors = DocumentMailboxCursors()
    runtime = _runtime()

    async with runtime.scope():
        ctx = runtime.get_context()
        with _bind(ctx):
            assert await cursors.get(ctx, principal="u1", client_key="d1") is None

            await cursors.advance(ctx, principal="u1", client_key="d1", up_to=_hlc(5))
            assert await cursors.get(ctx, principal="u1", client_key="d1") == _hlc(5)

            await cursors.advance(ctx, principal="u1", client_key="d1", up_to=_hlc(3))  # backwards
            assert await cursors.get(ctx, principal="u1", client_key="d1") == _hlc(5)

            await cursors.advance(ctx, principal="u1", client_key="d1", up_to=_hlc(8))
            assert await cursors.get(ctx, principal="u1", client_key="d1") == _hlc(8)


async def test_cursors_are_per_device_and_tenant() -> None:
    cursors = DocumentMailboxCursors()
    runtime = _runtime()

    async with runtime.scope():
        ctx = runtime.get_context()
        with _bind(ctx, _T1):
            await cursors.advance(ctx, principal="u1", client_key="d1", up_to=_hlc(5))
            assert await cursors.get(ctx, principal="u1", client_key="d2") is None
            assert await cursors.get(ctx, principal="u1", client_key="d1") == _hlc(5)
        with _bind(ctx, _T2):
            assert await cursors.get(ctx, principal="u1", client_key="d1") is None


async def test_min_cursor_is_lowest_across_devices() -> None:
    cursors = DocumentMailboxCursors()
    runtime = _runtime()

    async with runtime.scope():
        ctx = runtime.get_context()
        with _bind(ctx):
            assert await cursors.min_cursor(ctx, principal="u1") is None

            await cursors.advance(ctx, principal="u1", client_key="d1", up_to=_hlc(8))
            await cursors.advance(ctx, principal="u1", client_key="d2", up_to=_hlc(3))

            assert await cursors.min_cursor(ctx, principal="u1") == _hlc(3)
