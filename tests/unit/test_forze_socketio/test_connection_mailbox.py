"""Connect-time replay-since-cursor + realtime.ack (M3)."""

from __future__ import annotations

from typing import Any, cast
from uuid import UUID

from forze.application.contracts.authn import AuthnIdentity, ClientIdentity
from forze.application.contracts.realtime import Audience, RealtimeSignal
from forze.application.execution import (
    DepsRegistry,
    ExecutionContext,
    ExecutionRuntime,
)
from forze.base.primitives import HlcTimestamp
from forze_socketio import (
    InMemoryMailboxCursors,
    InMemoryRealtimeMailbox,
    RealtimeConnection,
    attach_realtime_connection,
)
from forze_socketio.routing import SocketIOConnect
from forze_mock import MockDepsModule

# ----------------------- #

_PRINCIPAL = UUID("22222222-2222-2222-2222-222222222222")
_TENANT = UUID("11111111-1111-1111-1111-111111111111")
_PRINCIPAL_STR = str(_PRINCIPAL)
_NULL_CTX = cast(ExecutionContext, None)


def _runtime() -> ExecutionRuntime:
    return ExecutionRuntime(deps=DepsRegistry.from_modules(MockDepsModule()).freeze())


def _hlc(physical_ms: int) -> HlcTimestamp:
    return HlcTimestamp(physical_ms=physical_ms, logical=0)


def _signal(text: str) -> RealtimeSignal:
    return RealtimeSignal.of(Audience.principal(_PRINCIPAL_STR), "order.shipped", {"text": text})


class _StubSio:
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
    await mailbox.store(_NULL_CTX, tenant=_TENANT, principal=_PRINCIPAL_STR, event_id="e1", hlc=_hlc(1), signal=_signal("a"))
    await mailbox.store(_NULL_CTX, tenant=_TENANT, principal=_PRINCIPAL_STR, event_id="e2", hlc=_hlc(2), signal=_signal("b"))


# ----------------------- #


async def test_connect_replays_pending_to_the_device() -> None:
    sio, mailbox, cursors = _StubSio(), InMemoryRealtimeMailbox(), InMemoryMailboxCursors()
    await _populate(mailbox)

    attach_realtime_connection(
        sio, resolve=_resolver(_connection()),  # pyright: ignore[reportArgumentType]
        mailbox=mailbox, cursors=cursors, runtime=_runtime(),
    )
    await sio.handlers["connect"]("sid-1", {}, {"device_id": "d1"})

    # both pending signals replayed to this socket, in order, in the uniform envelope
    assert [e["event"] for e in sio.emits] == ["order.shipped", "order.shipped"]
    assert [e["to"] for e in sio.emits] == ["sid-1", "sid-1"]
    assert sio.emits[0]["data"] == {"id": "e1", "data": {"text": "a"}}
    assert sio.emits[1]["data"] == {"id": "e2", "data": {"text": "b"}}


async def test_ack_advances_cursor_so_reconnect_replays_only_unseen() -> None:
    sio, mailbox, cursors = _StubSio(), InMemoryRealtimeMailbox(), InMemoryMailboxCursors()
    await _populate(mailbox)
    attach_realtime_connection(
        sio, resolve=_resolver(_connection()),  # pyright: ignore[reportArgumentType]
        mailbox=mailbox, cursors=cursors, runtime=_runtime(),
    )

    await sio.handlers["connect"]("sid-1", {}, None)  # first connect: replays e1, e2
    assert len(sio.emits) == 2

    await sio.handlers["realtime.ack"]("sid-1", {"up_to": "e2"})  # client acks through e2

    sio.emits.clear()
    await sio.handlers["connect"]("sid-1", {}, None)  # reconnect
    assert sio.emits == []  # nothing past the cursor


async def test_partial_ack_replays_only_the_tail_on_reconnect() -> None:
    sio, mailbox, cursors = _StubSio(), InMemoryRealtimeMailbox(), InMemoryMailboxCursors()
    await _populate(mailbox)
    attach_realtime_connection(
        sio, resolve=_resolver(_connection()),  # pyright: ignore[reportArgumentType]
        mailbox=mailbox, cursors=cursors, runtime=_runtime(),
    )

    await sio.handlers["connect"]("sid-1", {}, None)
    await sio.handlers["realtime.ack"]("sid-1", {"up_to": "e1"})  # acked only e1

    sio.emits.clear()
    await sio.handlers["connect"]("sid-1", {}, None)  # reconnect
    assert [e["data"]["id"] for e in sio.emits] == ["e2"]  # only the unacked tail


async def test_cursor_is_per_device() -> None:
    sio, mailbox, cursors = _StubSio(), InMemoryRealtimeMailbox(), InMemoryMailboxCursors()
    await _populate(mailbox)
    attach_realtime_connection(
        sio, resolve=_resolver(_connection(device_id="d1")),  # pyright: ignore[reportArgumentType]
        mailbox=mailbox, cursors=cursors, runtime=_runtime(),
    )

    await sio.handlers["connect"]("sid-1", {}, None)
    await sio.handlers["realtime.ack"]("sid-1", {"up_to": "e2"})  # d1 caught up

    # a different device (d2) still gets the full backlog
    sio.handlers.clear()
    sio.emits.clear()
    attach_realtime_connection(
        sio, resolve=_resolver(_connection(device_id="d2")),  # pyright: ignore[reportArgumentType]
        mailbox=mailbox, cursors=cursors, runtime=_runtime(),
    )
    await sio.handlers["connect"]("sid-2", {}, None)
    assert [e["data"]["id"] for e in sio.emits] == ["e1", "e2"]
