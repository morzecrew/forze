"""A connection's stored identity lasts as long as its namespace connection.

# covers: forze_socketio.connection (on_connect, on_disconnect, on_reauth, the connect's
#         undo, sweep_expired_connections)

python-socketio keeps a namespace's session inside its Engine.IO connection's session, so it
outlives a namespace disconnect: a client may connect the same namespace again over the same
transport. Run against python-socketio's own ``AsyncServer`` and manager, with python-engineio's
own socket and no transport behind it.
"""

from __future__ import annotations

import asyncio
import io
from collections.abc import Awaitable
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

import pytest
from engineio.async_socket import AsyncSocket
from socketio.async_server import AsyncServer

from forze.application.contracts.authn import AuthnIdentity
from forze.application.integrations.realtime import (
    REALTIME_REAUTH_EVENT,
    InMemoryRealtimePresence,
)
from forze_socketio import (
    CONNECTION_SESSION_KEY,
    IDENTITY_SESSION_KEY,
    RealtimeConnection,
    RealtimeConnectionStats,
    SocketIOConnect,
    attach_realtime_connection,
    sweep_expired_connections,
)

# ----------------------- #

_PRINCIPAL = UUID("22222222-2222-2222-2222-222222222222")
_ROOM = RealtimeConnection(authn=AuthnIdentity(principal_id=_PRINCIPAL)).principal_room


async def _resolve(connect: SocketIOConnect) -> RealtimeConnection | None:
    token = (connect.auth or {}).get("token")

    if token == "anon":
        return None

    expires_at = datetime(2000, 1, 1, tzinfo=UTC) if token == "expired" else None

    return RealtimeConnection(authn=AuthnIdentity(principal_id=_PRINCIPAL), expires_at=expires_at)


class _Held:
    """Resolves as :func:`_resolve` does, holding the one presenting *token* until released."""

    def __init__(self, token: str) -> None:
        self.token = token
        self.resolving = asyncio.Event()
        self.release = asyncio.Event()

    async def __call__(self, connect: SocketIOConnect) -> RealtimeConnection | None:
        if (connect.auth or {}).get("token") == self.token:
            self.resolving.set()
            await self.release.wait()

        return await _resolve(connect)


def _server() -> AsyncServer:
    server = AsyncServer()
    server.eio.sockets["eio-1"] = AsyncSocket(server.eio, "eio-1")
    server.environ["eio-1"] = {}

    return server


def _sids(server: AsyncServer, room: str | None = None) -> list[str]:
    return [sid for sid, _eio in server.manager.get_participants("/", room)]


async def _connect(server: AsyncServer, token: str) -> None:
    """A namespace connect over ``eio-1``, through python-socketio's own connect path."""

    await server._handle_connect("eio-1", "/", {"token": token})  # pyright: ignore[reportPrivateUsage]


async def _kept(server: AsyncServer) -> dict[str, Any]:
    """The identity ``/``'s session keeps on ``eio-1``: what its next connect starts with."""

    session = (await server.eio.get_session("eio-1")).get("/", {})

    return {
        key: session[key]
        for key in (IDENTITY_SESSION_KEY, CONNECTION_SESSION_KEY)
        if key in session
    }


# ----------------------- #


class _FailingPresence(InMemoryRealtimePresence):
    def __init__(self) -> None:
        super().__init__()
        self.fail = True

    async def joined(self, room: str, sid: str) -> None:
        if self.fail:
            self.fail = False
            raise RuntimeError("presence store down")

        await super().joined(room, sid)


class _FailingLeave(InMemoryRealtimePresence):
    """A presence store whose leave fails when awaited, or (*at_call*) when called."""

    def __init__(self, *, at_call: bool) -> None:
        super().__init__()
        self.at_call = at_call

    def left(self, room: str, sid: str) -> Awaitable[None]:  # type: ignore[override]  # as the protocol allows
        if self.at_call:
            raise RuntimeError("presence store down")

        return self._down()

    async def _down(self) -> None:
        raise RuntimeError("presence store down")


class TestIdentityEndsWithTheConnection:
    async def test_a_namespace_disconnect_drops_the_identity(self) -> None:
        server = _server()
        attach_realtime_connection(server, resolve=_resolve)
        await _connect(server, "ok")

        await server._handle_disconnect("eio-1", "/", "client namespace disconnect")  # pyright: ignore[reportPrivateUsage]

        assert await _kept(server) == {}

    # ....................... #

    @pytest.mark.parametrize("at_call", [False, True])
    async def test_a_leave_the_presence_store_fails_does_not_strand_the_connection(
        self, at_call: bool, error_log_buf: io.StringIO
    ) -> None:
        server = _server()
        attach_realtime_connection(
            server, resolve=_resolve, presence=_FailingLeave(at_call=at_call)
        )
        await _connect(server, "ok")

        await server._handle_disconnect("eio-1", "/", "client namespace disconnect")  # pyright: ignore[reportPrivateUsage]

        # The namespace still lets go of the sid; the store's entry lapses at its TTL.
        assert _sids(server) == []
        assert await _kept(server) == {}
        (record,) = error_log_buf.getvalue().splitlines()
        assert "presence store down" in record

    # ....................... #

    async def test_the_expiry_sweep_drops_the_identity(self) -> None:
        server = _server()
        attach_realtime_connection(server, resolve=_resolve)
        await _connect(server, "expired")

        assert await sweep_expired_connections(server, namespace="/") == 1
        assert "eio-1" in server.eio.sockets  # the sweep ends the namespace, not the transport

        assert await _kept(server) == {}

    # ....................... #

    async def test_a_connect_failing_to_join_its_room_is_refused_and_undone(self) -> None:
        server = _server()
        stats = RealtimeConnectionStats()
        attach_realtime_connection(server, resolve=_resolve, stats=stats)
        enter_room = server.enter_room
        failures = iter([RuntimeError("room join failed")])

        async def failing_once(sid: str, room: str, namespace: str | None = None) -> None:
            if (error := next(failures, None)) is not None:
                raise error

            await enter_room(sid, room, namespace=namespace)

        server.enter_room = failing_once  # type: ignore[method-assign]

        await _connect(server, "ok")  # refused: python-socketio answers it, nothing raises

        assert _sids(server) == []
        assert stats.connect_refused == {("/", "internal"): 1}
        assert await _kept(server) == {}

    # ....................... #

    async def test_a_connect_failing_to_record_presence_is_refused_and_undone(self) -> None:
        server = _server()
        presence = _FailingPresence()
        attach_realtime_connection(server, resolve=_resolve, presence=presence)

        await _connect(server, "ok")

        assert _sids(server) == []
        assert _sids(server, _ROOM) == []
        assert await presence.count(_ROOM) == 0
        assert await _kept(server) == {}

    # ....................... #

    @pytest.mark.parametrize("token", ["ok", "anon"])
    async def test_a_connect_starts_without_an_earlier_identity_and_stores_its_own(
        self, token: str
    ) -> None:
        server = _server()
        resolve = _Held(token)
        attach_realtime_connection(server, resolve=resolve)
        stale = RealtimeConnection(authn=AuthnIdentity(principal_id=UUID(int=7)))
        (await server.eio.get_session("eio-1"))["/"] = {  # as a disconnect that never ran left it
            IDENTITY_SESSION_KEY: stale.authn,
            CONNECTION_SESSION_KEY: stale,
        }

        connecting = asyncio.ensure_future(_connect(server, token))
        await asyncio.wait_for(resolve.resolving.wait(), timeout=1.0)
        during = await _kept(server)
        resolve.release.set()
        await asyncio.wait_for(connecting, timeout=1.0)

        assert during == {}
        expected = None if token == "anon" else AuthnIdentity(principal_id=_PRINCIPAL)
        kept = await _kept(server)
        assert kept.get(IDENTITY_SESSION_KEY) == expected
        assert getattr(kept.get(CONNECTION_SESSION_KEY), "authn", None) == expected

    # ....................... #

    async def test_a_reauth_finishing_after_its_connection_ended_stores_nothing(self) -> None:
        server = _server()
        resolve = _Held("fresh")
        attach_realtime_connection(server, resolve=resolve)
        await _connect(server, "ok")
        [sid] = _sids(server)
        reauth = asyncio.ensure_future(
            server.handlers["/"][REALTIME_REAUTH_EVENT](sid, {"token": "fresh"})
        )
        await asyncio.wait_for(resolve.resolving.wait(), timeout=1.0)

        await server._handle_disconnect("eio-1", "/", "client namespace disconnect")  # pyright: ignore[reportPrivateUsage]
        await _connect(server, "anon")
        resolve.release.set()
        ack = await asyncio.wait_for(reauth, timeout=1.0)

        assert "error" in ack
        assert await _kept(server) == {}

    # ....................... #

    async def test_a_reauth_overtaken_by_another_on_its_connection_stores_nothing(self) -> None:
        server = _server()
        resolve = _Held("fresh")
        attach_realtime_connection(server, resolve=resolve)
        await _connect(server, "ok")
        [sid] = _sids(server)
        reauth = server.handlers["/"][REALTIME_REAUTH_EVENT]
        slow = asyncio.ensure_future(reauth(sid, {"token": "fresh"}))
        await asyncio.wait_for(resolve.resolving.wait(), timeout=1.0)

        assert await reauth(sid, {"token": "ok"}) == {"ok": True}
        refreshed = (await _kept(server))[CONNECTION_SESSION_KEY]
        resolve.release.set()
        ack = await asyncio.wait_for(slow, timeout=1.0)

        assert ack["error"]["kind"] == "conflict"
        assert (await _kept(server))[CONNECTION_SESSION_KEY] is refreshed
