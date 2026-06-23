"""Tests for the realtime connection lifecycle — auto-join + presence (E5)."""

from __future__ import annotations

from typing import Any
from uuid import UUID

import pytest
from socketio.exceptions import ConnectionRefusedError as SocketIOConnectionRefusedError

from forze.application.contracts.authn import AuthnIdentity
from forze.base.exceptions import exc
from forze_socketio import (
    InMemoryRealtimePresence,
    RealtimeConnection,
    attach_realtime_connection,
)
from forze_socketio.connection import CONNECTION_SESSION_KEY
from forze_socketio.routing import IDENTITY_SESSION_KEY, SocketIOConnect

# ----------------------- #

_PRINCIPAL = UUID("22222222-2222-2222-2222-222222222222")
_TENANT = UUID("11111111-1111-1111-1111-111111111111")
_ROOM = f"t:{_TENANT}:principal:{_PRINCIPAL}"


class _StubSio:
    """Records handlers, rooms, and per-sid sessions."""

    def __init__(self) -> None:
        self.handlers: dict[str, Any] = {}
        self.entered: list[tuple[str, str]] = []
        self.sessions: dict[str, dict[str, Any]] = {}

    def on(self, event: str, handler: Any, namespace: str | None = None) -> None:
        self.handlers[event] = handler

    async def enter_room(self, sid: str, room: str, namespace: str | None = None) -> None:
        self.entered.append((sid, room))

    async def get_session(self, sid: str, namespace: str | None = None) -> dict[str, Any]:
        return self.sessions.setdefault(sid, {})

    async def save_session(self, sid: str, session: dict[str, Any], namespace: str | None = None) -> None:
        self.sessions[sid] = session


def _resolver(connection: RealtimeConnection | None):  # type: ignore[no-untyped-def]
    async def resolve(_c: SocketIOConnect) -> RealtimeConnection | None:
        return connection

    return resolve


def _connect_args(sid: str = "sid-1") -> tuple[str, dict, Any]:
    return sid, {"k": "v"}, {"token": "t"}


# ----------------------- #


async def test_connect_joins_principal_room_and_stores_identity() -> None:
    sio = _StubSio()
    presence = InMemoryRealtimePresence()
    connection = RealtimeConnection(authn=AuthnIdentity(principal_id=_PRINCIPAL), tenant=_TENANT)

    attach_realtime_connection(sio, resolve=_resolver(connection), presence=presence)  # pyright: ignore[reportArgumentType]
    await sio.handlers["connect"](*_connect_args())

    assert sio.entered == [("sid-1", _ROOM)]
    assert sio.sessions["sid-1"][IDENTITY_SESSION_KEY].principal_id == _PRINCIPAL
    assert sio.sessions["sid-1"][CONNECTION_SESSION_KEY] is connection
    assert await presence.count(_ROOM) == 1


async def test_disconnect_updates_presence() -> None:
    sio = _StubSio()
    presence = InMemoryRealtimePresence()
    connection = RealtimeConnection(authn=AuthnIdentity(principal_id=_PRINCIPAL), tenant=_TENANT)

    attach_realtime_connection(sio, resolve=_resolver(connection), presence=presence)  # pyright: ignore[reportArgumentType]
    await sio.handlers["connect"](*_connect_args())
    assert await presence.count(_ROOM) == 1

    await sio.handlers["disconnect"]("sid-1")
    assert await presence.count(_ROOM) == 0


async def test_anonymous_connection_joins_nothing() -> None:
    sio = _StubSio()
    presence = InMemoryRealtimePresence()

    attach_realtime_connection(sio, resolve=_resolver(None), presence=presence)  # pyright: ignore[reportArgumentType]
    await sio.handlers["connect"](*_connect_args())

    assert sio.entered == []
    assert "sid-1" not in sio.sessions or not sio.sessions["sid-1"]


async def test_resolver_can_refuse_connection() -> None:
    sio = _StubSio()

    async def refusing(_c: SocketIOConnect) -> RealtimeConnection | None:
        raise exc.authentication("bad token")

    attach_realtime_connection(sio, resolve=refusing)  # pyright: ignore[reportArgumentType]

    with pytest.raises(SocketIOConnectionRefusedError):
        await sio.handlers["connect"](*_connect_args())


async def test_presence_counts_multiple_connections() -> None:
    presence = InMemoryRealtimePresence()

    await presence.joined(_ROOM, "sid-a")
    await presence.joined(_ROOM, "sid-b")
    assert await presence.count(_ROOM) == 2

    await presence.left(_ROOM, "sid-a")
    assert await presence.count(_ROOM) == 1
    assert await presence.count("t:x:principal:nobody") == 0
