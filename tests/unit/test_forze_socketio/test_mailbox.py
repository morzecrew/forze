"""Mailbox + cursor seams — in-memory shape (principal-keyed, no ctx/tenant)."""

from __future__ import annotations

from forze.application.contracts.authn import ClientIdentity
from forze.application.contracts.realtime import Audience, RealtimeSignal
from forze.base.primitives import HlcTimestamp
from forze_socketio import (
    InMemoryMailboxCursors,
    InMemoryRealtimeMailbox,
    MailboxCursors,
    RealtimeMailbox,
)

# ----------------------- #


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
# RealtimeMailbox (in-memory)


async def test_store_read_since_is_ordered_and_filtered() -> None:
    mb = InMemoryRealtimeMailbox()
    assert isinstance(mb, RealtimeMailbox)

    await mb.store(principal="u1", event_id="e2", hlc=_hlc(2), signal=_signal("b"))
    await mb.store(principal="u1", event_id="e1", hlc=_hlc(1), signal=_signal("a"))
    await mb.store(principal="u1", event_id="e3", hlc=_hlc(3), signal=_signal("c"))

    everything = await mb.read_since(principal="u1", since=None)
    after_e1 = await mb.read_since(principal="u1", since=_hlc(1))

    assert [e.event_id for e in everything] == ["e1", "e2", "e3"]
    assert everything[0].event == "order.shipped"
    assert everything[0].payload == {"text": "a"}
    assert [e.event_id for e in after_e1] == ["e2", "e3"]  # strictly after


async def test_replay_since_streams_same_order_as_read_since() -> None:
    mb = InMemoryRealtimeMailbox()
    await mb.store(principal="u1", event_id="e2", hlc=_hlc(2), signal=_signal("b"))
    await mb.store(principal="u1", event_id="e1", hlc=_hlc(1), signal=_signal("a"))
    await mb.store(principal="u1", event_id="e3", hlc=_hlc(3), signal=_signal("c"))

    streamed = [e.event_id async for e in mb.replay_since(principal="u1", since=None)]
    after = [e.event_id async for e in mb.replay_since(principal="u1", since=_hlc(1))]

    assert streamed == ["e1", "e2", "e3"]
    assert after == ["e2", "e3"]


async def test_store_is_idempotent_on_event_id() -> None:
    mb = InMemoryRealtimeMailbox()

    await mb.store(principal="u1", event_id="e1", hlc=_hlc(1), signal=_signal("a"))
    await mb.store(principal="u1", event_id="e1", hlc=_hlc(1), signal=_signal("a"))

    assert len(await mb.read_since(principal="u1", since=None)) == 1


async def test_mailbox_is_scoped_by_principal() -> None:
    mb = InMemoryRealtimeMailbox()

    await mb.store(principal="u1", event_id="e1", hlc=_hlc(1), signal=_signal("a"))

    assert [e.event_id for e in await mb.read_since(principal="u1", since=None)] == ["e1"]
    assert await mb.read_since(principal="u2", since=None) == []


async def test_position_of_and_trim() -> None:
    mb = InMemoryRealtimeMailbox()
    for i in (1, 2, 3):
        await mb.store(principal="u1", event_id=f"e{i}", hlc=_hlc(i), signal=_signal(str(i)))

    assert await mb.position_of(principal="u1", event_id="e2") == _hlc(2)
    assert await mb.position_of(principal="u1", event_id="missing") is None

    await mb.trim(principal="u1", before=_hlc(2))
    assert [e.event_id for e in await mb.read_since(principal="u1", since=None)] == ["e3"]


# ....................... #
# MailboxCursors (in-memory)


async def test_cursor_get_advance_is_monotonic() -> None:
    cursors = InMemoryMailboxCursors()
    assert isinstance(cursors, MailboxCursors)

    assert await cursors.get(principal="u1", client_key="d1") is None

    await cursors.advance(principal="u1", client_key="d1", up_to=_hlc(5))
    assert await cursors.get(principal="u1", client_key="d1") == _hlc(5)

    await cursors.advance(principal="u1", client_key="d1", up_to=_hlc(3))  # backwards
    assert await cursors.get(principal="u1", client_key="d1") == _hlc(5)  # held

    await cursors.advance(principal="u1", client_key="d1", up_to=_hlc(8))
    assert await cursors.get(principal="u1", client_key="d1") == _hlc(8)


async def test_cursors_are_per_device_and_min_cursor() -> None:
    cursors = InMemoryMailboxCursors()

    assert await cursors.min_cursor(principal="u1") is None

    await cursors.advance(principal="u1", client_key="d1", up_to=_hlc(8))
    await cursors.advance(principal="u1", client_key="d2", up_to=_hlc(3))
    await cursors.advance(principal="u2", client_key="d1", up_to=_hlc(1))

    assert await cursors.get(principal="u1", client_key="d2") == _hlc(3)
    assert await cursors.min_cursor(principal="u1") == _hlc(3)  # slowest of u1's devices
