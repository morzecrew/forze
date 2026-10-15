"""Connect-time replay-since-cursor + realtime.ack (M3)."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any
from uuid import UUID

from forze.application.contracts.authn import AuthnIdentity, ClientIdentity
from forze.application.contracts.realtime import Audience, RealtimeSignal
from forze.application.execution import DepsRegistry, ExecutionRuntime
from forze.base.primitives import HlcTimestamp
from forze_mock import MockDepsModule
from forze_socketio import (
    InMemoryMailboxCursors,
    InMemoryRealtimeMailbox,
    RealtimeConnection,
    attach_realtime_connection,
)
from forze_socketio.routing import SocketIOConnect

# ----------------------- #

_PRINCIPAL = UUID("22222222-2222-2222-2222-222222222222")
_TENANT = UUID("11111111-1111-1111-1111-111111111111")
_PRINCIPAL_STR = str(_PRINCIPAL)


def _hlc(physical_ms: int) -> HlcTimestamp:
    return HlcTimestamp(physical_ms=physical_ms, logical=0)


def _signal(text: str) -> RealtimeSignal:
    return RealtimeSignal.of(Audience.principal(_PRINCIPAL_STR), "order.shipped", {"text": text})


class _StubSio:
    manager = SimpleNamespace(
        is_connected=lambda _sid, _namespace: True, eio_sid_from_sid=lambda _sid, _namespace: None
    )
    """Every sid stays connected, on no Engine.IO connection the stub models: the connect
    checks before each stage that it still is, and records what it accepted on it."""

    def __init__(self) -> None:
        self.handlers: dict[str, Any] = {}
        self.sessions: dict[str, dict[str, Any]] = {}
        self.emits: list[dict[str, Any]] = []

    def on(self, event: str, handler: Any, namespace: str | None = None) -> None:
        self.handlers[event] = handler

    async def enter_room(self, sid: str, room: str, namespace: str | None = None) -> None: ...

    async def get_session(self, sid: str, namespace: str | None = None) -> dict[str, Any]:
        return self.sessions.setdefault(sid, {})

    async def save_session(self, sid: str, session: dict[str, Any], namespace: str | None = None) -> None:
        self.sessions[sid] = session

    async def emit(self, event: str, data: Any = None, *, to: str | None = None,
                   room: str | None = None, namespace: str | None = None, **_: Any) -> None:
        self.emits.append({"event": event, "data": data, "to": to})


def _connection(*, device_id: str | None = "d1") -> RealtimeConnection:
    return RealtimeConnection(
        authn=AuthnIdentity(principal_id=_PRINCIPAL),
        tenant=_TENANT,
        client=ClientIdentity(device_id=device_id),
    )


def _resolver(connection: RealtimeConnection):  # type: ignore[no-untyped-def]
    async def resolve(_c: SocketIOConnect) -> RealtimeConnection:
        return connection

    return resolve


async def _populate(mailbox: InMemoryRealtimeMailbox) -> None:
    await mailbox.store(principal=_PRINCIPAL_STR, event_id="e1", hlc=_hlc(1), signal=_signal("a"))
    await mailbox.store(principal=_PRINCIPAL_STR, event_id="e2", hlc=_hlc(2), signal=_signal("b"))


# ----------------------- #


async def test_connect_replays_pending_to_the_device(serving_runtime: ExecutionRuntime) -> None:
    sio, mailbox, cursors = _StubSio(), InMemoryRealtimeMailbox(), InMemoryMailboxCursors()
    await _populate(mailbox)

    attach_realtime_connection(
        sio, resolve=_resolver(_connection()),  # pyright: ignore[reportArgumentType]
        mailbox_factory=lambda ctx: mailbox, cursors_factory=lambda ctx: cursors, runtime=serving_runtime,
    )
    await sio.handlers["connect"]("sid-1", {}, {"device_id": "d1"})

    # both pending signals replayed to this socket, in order, in the uniform envelope
    assert [e["event"] for e in sio.emits] == ["order.shipped", "order.shipped"]
    assert [e["to"] for e in sio.emits] == ["sid-1", "sid-1"]
    assert sio.emits[0]["data"] == {"id": "e1", "data": {"text": "a"}}
    assert sio.emits[1]["data"] == {"id": "e2", "data": {"text": "b"}}


async def test_ack_advances_cursor_so_reconnect_replays_only_unseen(serving_runtime: ExecutionRuntime) -> None:
    sio, mailbox, cursors = _StubSio(), InMemoryRealtimeMailbox(), InMemoryMailboxCursors()
    await _populate(mailbox)
    attach_realtime_connection(
        sio, resolve=_resolver(_connection()),  # pyright: ignore[reportArgumentType]
        mailbox_factory=lambda ctx: mailbox, cursors_factory=lambda ctx: cursors, runtime=serving_runtime,
    )

    await sio.handlers["connect"]("sid-1", {}, None)  # first connect: replays e1, e2
    assert len(sio.emits) == 2

    await sio.handlers["realtime.ack"]("sid-1", {"up_to": "e2"})  # client acks through e2

    sio.emits.clear()
    await sio.handlers["connect"]("sid-1", {}, None)  # reconnect
    assert sio.emits == []  # nothing past the cursor


async def test_partial_ack_replays_only_the_tail_on_reconnect(serving_runtime: ExecutionRuntime) -> None:
    sio, mailbox, cursors = _StubSio(), InMemoryRealtimeMailbox(), InMemoryMailboxCursors()
    await _populate(mailbox)
    attach_realtime_connection(
        sio, resolve=_resolver(_connection()),  # pyright: ignore[reportArgumentType]
        mailbox_factory=lambda ctx: mailbox, cursors_factory=lambda ctx: cursors, runtime=serving_runtime,
    )

    await sio.handlers["connect"]("sid-1", {}, None)
    await sio.handlers["realtime.ack"]("sid-1", {"up_to": "e1"})  # acked only e1

    sio.emits.clear()
    await sio.handlers["connect"]("sid-1", {}, None)  # reconnect
    assert [e["data"]["id"] for e in sio.emits] == ["e2"]  # only the unacked tail


async def test_ack_trims_what_the_only_device_has_acked(serving_runtime: ExecutionRuntime) -> None:
    sio, mailbox, cursors = _StubSio(), InMemoryRealtimeMailbox(), InMemoryMailboxCursors()
    await _populate(mailbox)
    attach_realtime_connection(
        sio, resolve=_resolver(_connection()),  # pyright: ignore[reportArgumentType]
        mailbox_factory=lambda ctx: mailbox, cursors_factory=lambda ctx: cursors, runtime=serving_runtime,
    )

    await sio.handlers["connect"]("sid-1", {}, None)
    await sio.handlers["realtime.ack"]("sid-1", {"up_to": "e2"})  # the only device acked all

    remaining = await mailbox.read_since(principal=_PRINCIPAL_STR, since=None)

    assert remaining == []  # every known device acked through e2 → trimmed


async def test_ack_keeps_entries_a_slower_device_has_not_acked(serving_runtime: ExecutionRuntime) -> None:
    sio, mailbox, cursors = _StubSio(), InMemoryRealtimeMailbox(), InMemoryMailboxCursors()
    await _populate(mailbox)

    # device d1 acks through e1
    attach_realtime_connection(
        sio, resolve=_resolver(_connection(device_id="d1")),  # pyright: ignore[reportArgumentType]
        mailbox_factory=lambda ctx: mailbox, cursors_factory=lambda ctx: cursors, runtime=serving_runtime,
    )
    await sio.handlers["connect"]("sid-1", {}, None)
    await sio.handlers["realtime.ack"]("sid-1", {"up_to": "e1"})  # d1 establishes a cursor at e1

    # device d2 acks only through e1 → min cursor is e1 → only e1 trimmed
    sio.handlers.clear()
    attach_realtime_connection(
        sio, resolve=_resolver(_connection(device_id="d2")),  # pyright: ignore[reportArgumentType]
        mailbox_factory=lambda ctx: mailbox, cursors_factory=lambda ctx: cursors, runtime=serving_runtime,
    )
    await sio.handlers["connect"]("sid-2", {}, None)
    await sio.handlers["realtime.ack"]("sid-2", {"up_to": "e1"})

    remaining = await mailbox.read_since(principal=_PRINCIPAL_STR, since=None)

    assert [e.event_id for e in remaining] == ["e2"]  # e2 retained until both ack it


async def test_new_device_after_trim_gets_only_retained_entries(serving_runtime: ExecutionRuntime) -> None:
    # M4 trim semantics: once the only known device acks everything, the mailbox is
    # trimmed; a device that connects afterwards is "unknown" and gets the retained
    # window only (here: nothing) — the documented TTL-window bound.
    sio, mailbox, cursors = _StubSio(), InMemoryRealtimeMailbox(), InMemoryMailboxCursors()
    await _populate(mailbox)
    attach_realtime_connection(
        sio, resolve=_resolver(_connection(device_id="d1")),  # pyright: ignore[reportArgumentType]
        mailbox_factory=lambda ctx: mailbox, cursors_factory=lambda ctx: cursors, runtime=serving_runtime,
    )

    await sio.handlers["connect"]("sid-1", {}, None)
    await sio.handlers["realtime.ack"]("sid-1", {"up_to": "e2"})  # sole device caught up → trimmed

    sio.handlers.clear()
    sio.emits.clear()
    attach_realtime_connection(
        sio, resolve=_resolver(_connection(device_id="d2")),  # pyright: ignore[reportArgumentType]
        mailbox_factory=lambda ctx: mailbox, cursors_factory=lambda ctx: cursors, runtime=serving_runtime,
    )
    await sio.handlers["connect"]("sid-2", {}, None)
    assert sio.emits == []  # backlog was trimmed before d2 was known


async def test_connects_replaying_at_once_each_get_their_replay(
    serving_runtime: ExecutionRuntime,
) -> None:
    # Replay runs on the serving runtime's live context, which every connect shares; none
    # opens a scope of its own, so connects replaying at the same time do not collide.
    class _SlowCursors(InMemoryMailboxCursors):
        async def get(self, **kwargs: Any) -> HlcTimestamp | None:
            await asyncio.sleep(0.005)  # every replay is under way before any finishes
            return await super().get(**kwargs)

    sio, mailbox, cursors = _StubSio(), InMemoryRealtimeMailbox(), _SlowCursors()
    await _populate(mailbox)
    attach_realtime_connection(
        sio, resolve=_resolver(_connection()),  # pyright: ignore[reportArgumentType]
        mailbox_factory=lambda ctx: mailbox, cursors_factory=lambda ctx: cursors, runtime=serving_runtime,
    )

    await asyncio.gather(*(sio.handlers["connect"](f"sid-{n}", {}, None) for n in range(50)))

    for n in range(50):
        assert [e["data"]["id"] for e in sio.emits if e["to"] == f"sid-{n}"] == ["e1", "e2"]


async def test_with_the_serving_scope_closed_replay_is_logged_and_acks_are_ignored(
    error_log_buf: Any,
) -> None:
    sio, mailbox, cursors = _StubSio(), InMemoryRealtimeMailbox(), InMemoryMailboxCursors()
    await _populate(mailbox)
    closed = ExecutionRuntime(deps=DepsRegistry.from_modules(MockDepsModule()).freeze())
    attach_realtime_connection(
        sio, resolve=_resolver(_connection()),  # pyright: ignore[reportArgumentType]
        mailbox_factory=lambda ctx: mailbox, cursors_factory=lambda ctx: cursors, runtime=closed,
    )

    await sio.handlers["connect"]("sid-1", {}, None)  # accepted: replay is best-effort
    await sio.handlers["realtime.ack"]("sid-1", {"up_to": "e2"})

    assert sio.emits == []
    assert await cursors.get(principal=_PRINCIPAL_STR, client_key="d1") is None
    assert "realtime_replay_scope_closed" in error_log_buf.getvalue()
