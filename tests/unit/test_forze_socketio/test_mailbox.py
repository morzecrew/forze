"""Mailbox + cursor seams — in-memory shape (M0)."""

from __future__ import annotations

from typing import cast
from uuid import UUID

from forze.application.contracts.authn import ClientIdentity
from forze.application.contracts.realtime import Audience, RealtimeSignal
from forze.application.execution import ExecutionContext
from forze.base.primitives import HlcTimestamp
from forze_socketio import (
    InMemoryMailboxCursors,
    InMemoryRealtimeMailbox,
    MailboxCursors,
    RealtimeMailbox,
)

# ----------------------- #

_TENANT = UUID("11111111-1111-1111-1111-111111111111")
_CTX = cast(ExecutionContext, None)  # the in-memory impls ignore ctx


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

    # stored out of order; read_since returns oldest-first
    await mb.store(_CTX, tenant=_TENANT, principal="u1", event_id="e2", hlc=_hlc(2), signal=_signal("b"))
    await mb.store(_CTX, tenant=_TENANT, principal="u1", event_id="e1", hlc=_hlc(1), signal=_signal("a"))
    await mb.store(_CTX, tenant=_TENANT, principal="u1", event_id="e3", hlc=_hlc(3), signal=_signal("c"))

    everything = await mb.read_since(_CTX, tenant=_TENANT, principal="u1", since=None)
    assert [e.event_id for e in everything] == ["e1", "e2", "e3"]
    assert everything[0].event == "order.shipped"
    assert everything[0].payload == {"text": "a"}

    after_e1 = await mb.read_since(_CTX, tenant=_TENANT, principal="u1", since=_hlc(1))
    assert [e.event_id for e in after_e1] == ["e2", "e3"]  # strictly after


async def test_store_is_idempotent_on_event_id() -> None:
    mb = InMemoryRealtimeMailbox()

    await mb.store(_CTX, tenant=_TENANT, principal="u1", event_id="e1", hlc=_hlc(1), signal=_signal("a"))
    await mb.store(_CTX, tenant=_TENANT, principal="u1", event_id="e1", hlc=_hlc(1), signal=_signal("a"))

    rows = await mb.read_since(_CTX, tenant=_TENANT, principal="u1", since=None)
    assert len(rows) == 1


async def test_mailbox_is_scoped_by_tenant_and_principal() -> None:
    mb = InMemoryRealtimeMailbox()

    await mb.store(_CTX, tenant=_TENANT, principal="u1", event_id="e1", hlc=_hlc(1), signal=_signal("a"))

    assert await mb.read_since(_CTX, tenant=_TENANT, principal="u2", since=None) == []
    assert await mb.read_since(_CTX, tenant=None, principal="u1", since=None) == []


async def test_trim_drops_entries_at_or_before_cutoff() -> None:
    mb = InMemoryRealtimeMailbox()
    for i in (1, 2, 3):
        await mb.store(_CTX, tenant=_TENANT, principal="u1", event_id=f"e{i}", hlc=_hlc(i), signal=_signal(str(i)))

    await mb.trim(_CTX, tenant=_TENANT, principal="u1", before=_hlc(2))

    rows = await mb.read_since(_CTX, tenant=_TENANT, principal="u1", since=None)
    assert [e.event_id for e in rows] == ["e3"]  # e1, e2 dropped


# ....................... #
# MailboxCursors


async def test_cursor_get_advance_is_monotonic() -> None:
    cursors = InMemoryMailboxCursors()
    assert isinstance(cursors, MailboxCursors)

    assert await cursors.get(_CTX, tenant=_TENANT, principal="u1", client_key="d1") is None

    await cursors.advance(_CTX, tenant=_TENANT, principal="u1", client_key="d1", up_to=_hlc(5))
    assert await cursors.get(_CTX, tenant=_TENANT, principal="u1", client_key="d1") == _hlc(5)

    await cursors.advance(_CTX, tenant=_TENANT, principal="u1", client_key="d1", up_to=_hlc(3))  # backwards
    assert await cursors.get(_CTX, tenant=_TENANT, principal="u1", client_key="d1") == _hlc(5)  # held

    await cursors.advance(_CTX, tenant=_TENANT, principal="u1", client_key="d1", up_to=_hlc(8))
    assert await cursors.get(_CTX, tenant=_TENANT, principal="u1", client_key="d1") == _hlc(8)


async def test_cursors_are_per_device() -> None:
    cursors = InMemoryMailboxCursors()

    await cursors.advance(_CTX, tenant=_TENANT, principal="u1", client_key="d1", up_to=_hlc(5))

    assert await cursors.get(_CTX, tenant=_TENANT, principal="u1", client_key="d2") is None
    assert await cursors.get(_CTX, tenant=_TENANT, principal="u1", client_key="d1") == _hlc(5)
