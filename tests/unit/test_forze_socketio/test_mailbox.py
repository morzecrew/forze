"""Mailbox + cursor seams — in-memory shape, tenant read ambiently (M0)."""

from __future__ import annotations

from contextlib import AbstractContextManager
from uuid import UUID

from forze.application.contracts.authn import ClientIdentity
from forze.application.contracts.realtime import Audience, RealtimeSignal
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import DepsRegistry, ExecutionContext, ExecutionRuntime
from forze.base.primitives import HlcTimestamp
from forze_socketio import (
    InMemoryMailboxCursors,
    InMemoryRealtimeMailbox,
    MailboxCursors,
    RealtimeMailbox,
)
from forze_mock import MockDepsModule

# ----------------------- #

_T1 = UUID("11111111-1111-1111-1111-111111111111")
_T2 = UUID("22222222-2222-2222-2222-222222222222")


def _runtime() -> ExecutionRuntime:
    return ExecutionRuntime(deps=DepsRegistry.from_modules(MockDepsModule()).freeze())


def _bind(ctx: ExecutionContext, tenant: UUID = _T1) -> AbstractContextManager[None]:
    """The tenant is ambient — bound by the worker, never passed to the mailbox."""

    return ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=tenant))


def _hlc(physical_ms: int, logical: int = 0) -> HlcTimestamp:
    return HlcTimestamp(physical_ms=physical_ms, logical=logical)


def _signal(text: str) -> RealtimeSignal:
    return RealtimeSignal.of(Audience.principal("u1"), "order.shipped", {"text": text})


# ----------------------- #
# ClientIdentity


def test_client_identity_key_prefers_device_then_session() -> None:
    assert ClientIdentity(device_id="d1", session_id="s1").key == "d1"
    assert ClientIdentity(session_id="s1").key == "s1"
    assert ClientIdentity(device_id="d1").key == "d1"
    assert ClientIdentity().key is None


# ....................... #
# RealtimeMailbox


async def test_store_read_since_is_ordered_and_filtered() -> None:
    mb = InMemoryRealtimeMailbox()
    assert isinstance(mb, RealtimeMailbox)

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
    mb = InMemoryRealtimeMailbox()
    runtime = _runtime()

    async with runtime.scope():
        ctx = runtime.get_context()
        with _bind(ctx):
            await mb.store(ctx, principal="u1", event_id="e1", hlc=_hlc(1), signal=_signal("a"))
            await mb.store(ctx, principal="u1", event_id="e1", hlc=_hlc(1), signal=_signal("a"))
            rows = await mb.read_since(ctx, principal="u1", since=None)

    assert len(rows) == 1


async def test_mailbox_is_scoped_by_ambient_tenant_and_principal() -> None:
    mb = InMemoryRealtimeMailbox()
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
    assert other_principal == []
    assert other_tenant == []  # a different ambient tenant sees nothing


async def test_trim_drops_entries_at_or_before_cutoff() -> None:
    mb = InMemoryRealtimeMailbox()
    runtime = _runtime()

    async with runtime.scope():
        ctx = runtime.get_context()
        with _bind(ctx):
            for i in (1, 2, 3):
                await mb.store(ctx, principal="u1", event_id=f"e{i}", hlc=_hlc(i), signal=_signal(str(i)))

            await mb.trim(ctx, principal="u1", before=_hlc(2))
            rows = await mb.read_since(ctx, principal="u1", since=None)

    assert [e.event_id for e in rows] == ["e3"]  # e1, e2 dropped


# ....................... #
# MailboxCursors


async def test_cursor_get_advance_is_monotonic() -> None:
    cursors = InMemoryMailboxCursors()
    assert isinstance(cursors, MailboxCursors)
    runtime = _runtime()

    async with runtime.scope():
        ctx = runtime.get_context()
        with _bind(ctx):
            assert await cursors.get(ctx, principal="u1", client_key="d1") is None

            await cursors.advance(ctx, principal="u1", client_key="d1", up_to=_hlc(5))
            assert await cursors.get(ctx, principal="u1", client_key="d1") == _hlc(5)

            await cursors.advance(ctx, principal="u1", client_key="d1", up_to=_hlc(3))  # backwards
            assert await cursors.get(ctx, principal="u1", client_key="d1") == _hlc(5)  # held

            await cursors.advance(ctx, principal="u1", client_key="d1", up_to=_hlc(8))
            assert await cursors.get(ctx, principal="u1", client_key="d1") == _hlc(8)


async def test_cursors_are_per_device() -> None:
    cursors = InMemoryMailboxCursors()
    runtime = _runtime()

    async with runtime.scope():
        ctx = runtime.get_context()
        with _bind(ctx):
            await cursors.advance(ctx, principal="u1", client_key="d1", up_to=_hlc(5))

            assert await cursors.get(ctx, principal="u1", client_key="d2") is None
            assert await cursors.get(ctx, principal="u1", client_key="d1") == _hlc(5)


async def test_min_cursor_is_lowest_across_known_devices() -> None:
    cursors = InMemoryMailboxCursors()
    runtime = _runtime()

    async with runtime.scope():
        ctx = runtime.get_context()
        with _bind(ctx):
            assert await cursors.min_cursor(ctx, principal="u1") is None

            await cursors.advance(ctx, principal="u1", client_key="d1", up_to=_hlc(8))
            await cursors.advance(ctx, principal="u1", client_key="d2", up_to=_hlc(3))
            await cursors.advance(ctx, principal="u2", client_key="d1", up_to=_hlc(1))

            # the slowest of u1's devices (not u2's)
            assert await cursors.min_cursor(ctx, principal="u1") == _hlc(3)
