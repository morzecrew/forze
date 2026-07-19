"""Tests for the realtime connection lifecycle — auto-join + presence (E5)."""

from __future__ import annotations

from datetime import UTC, datetime
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


# ----------------------- #
# realtime.reauth — refresh credentials in place


async def test_reauth_swaps_identity_and_expiry_in_place() -> None:
    sio = _StubSio()
    first = RealtimeConnection(
        authn=AuthnIdentity(principal_id=_PRINCIPAL),
        tenant=_TENANT,
        expires_at=datetime(2026, 1, 1, tzinfo=UTC),
    )
    refreshed = RealtimeConnection(
        authn=AuthnIdentity(principal_id=_PRINCIPAL),
        tenant=_TENANT,
        expires_at=datetime(2027, 1, 1, tzinfo=UTC),
    )
    outcomes = iter([first, refreshed])

    async def resolve(_c: SocketIOConnect) -> RealtimeConnection | None:
        return next(outcomes)

    attach_realtime_connection(sio, resolve=resolve)  # pyright: ignore[reportArgumentType]
    await sio.handlers["connect"](*_connect_args())

    ack = await sio.handlers["realtime.reauth"]("sid-1", {"token": "fresh"})

    assert ack == {"ok": True}
    stored: RealtimeConnection = sio.sessions["sid-1"][CONNECTION_SESSION_KEY]
    assert stored is refreshed  # identity + expires_at swapped without a reconnect
    assert sio.entered == [("sid-1", _ROOM)]  # no re-join — same principal, same room


async def test_reauth_refuses_a_different_principal() -> None:
    sio = _StubSio()
    first = RealtimeConnection(authn=AuthnIdentity(principal_id=_PRINCIPAL), tenant=_TENANT)
    other = RealtimeConnection(
        authn=AuthnIdentity(principal_id=UUID("33333333-3333-3333-3333-333333333333")),
        tenant=_TENANT,
    )
    outcomes = iter([first, other])

    async def resolve(_c: SocketIOConnect) -> RealtimeConnection | None:
        return next(outcomes)

    attach_realtime_connection(sio, resolve=resolve)  # pyright: ignore[reportArgumentType]
    await sio.handlers["connect"](*_connect_args())

    ack = await sio.handlers["realtime.reauth"]("sid-1", {"token": "other-user"})

    assert "error" in ack  # a different principal is a re-login → reconnect
    stored: RealtimeConnection = sio.sessions["sid-1"][CONNECTION_SESSION_KEY]
    assert stored is first  # nothing swapped


async def test_reauth_with_bad_token_returns_error_ack_and_keeps_identity() -> None:
    sio = _StubSio()
    first = RealtimeConnection(authn=AuthnIdentity(principal_id=_PRINCIPAL), tenant=_TENANT)
    calls = {"n": 0}

    async def resolve(_c: SocketIOConnect) -> RealtimeConnection | None:
        calls["n"] += 1
        if calls["n"] == 1:
            return first
        raise exc.authentication("bad token")

    attach_realtime_connection(sio, resolve=resolve)  # pyright: ignore[reportArgumentType]
    await sio.handlers["connect"](*_connect_args())

    ack = await sio.handlers["realtime.reauth"]("sid-1", {"token": "expired"})

    assert ack["error"]["kind"] == "authentication"
    assert sio.sessions["sid-1"][CONNECTION_SESSION_KEY] is first


async def test_reauth_on_unauthenticated_connection_is_refused() -> None:
    sio = _StubSio()

    attach_realtime_connection(sio, resolve=_resolver(None))  # pyright: ignore[reportArgumentType]
    await sio.handlers["connect"](*_connect_args())  # anonymous — nothing stored

    ack = await sio.handlers["realtime.reauth"]("sid-1", {"token": "t"})

    assert "error" in ack


async def test_reauth_refuses_a_tenant_change() -> None:
    sio = _StubSio()
    first = RealtimeConnection(authn=AuthnIdentity(principal_id=_PRINCIPAL), tenant=_TENANT)
    moved = RealtimeConnection(
        authn=AuthnIdentity(principal_id=_PRINCIPAL),  # same principal...
        tenant=UUID("99999999-9999-9999-9999-999999999999"),  # ...new tenant
    )
    outcomes = iter([first, moved])

    async def resolve(_c: SocketIOConnect) -> RealtimeConnection | None:
        return next(outcomes)

    attach_realtime_connection(sio, resolve=resolve)  # pyright: ignore[reportArgumentType]
    await sio.handlers["connect"](*_connect_args())

    ack = await sio.handlers["realtime.reauth"]("sid-1", {"token": "other-tenant"})

    # the socket is still in the OLD tenant's rooms — swapping identity in place would
    # deliver the old tenant's events under the new tenant's authority
    assert "error" in ack
    assert sio.sessions["sid-1"][CONNECTION_SESSION_KEY] is first


async def test_reauth_survives_a_crashing_resolver_with_a_generic_ack() -> None:
    sio = _StubSio()
    first = RealtimeConnection(authn=AuthnIdentity(principal_id=_PRINCIPAL), tenant=_TENANT)
    calls = {"n": 0}

    async def resolve(_c: SocketIOConnect) -> RealtimeConnection | None:
        calls["n"] += 1
        if calls["n"] == 1:
            return first
        raise RuntimeError("verifier exploded")

    attach_realtime_connection(sio, resolve=resolve)  # pyright: ignore[reportArgumentType]
    await sio.handlers["connect"](*_connect_args())

    ack = await sio.handlers["realtime.reauth"]("sid-1", {"token": "t"})

    assert ack["error"]["kind"] == "internal"  # generic — internals never leak
    assert sio.sessions["sid-1"][CONNECTION_SESSION_KEY] is first
