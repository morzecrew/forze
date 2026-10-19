"""Connection hygiene — credential-expiry sweep + presence heartbeat (hardening)."""

from __future__ import annotations

import asyncio
import io
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any, cast
from uuid import UUID

from engineio.async_socket import AsyncSocket
from socketio.async_server import AsyncServer

from forze.application.contracts.authn import AuthnIdentity
from forze.application.execution import ExecutionContext
from forze.application.execution.background.periodic import (  # pyright: ignore[reportPrivateUsage]
    _PeriodicShutdown,
    _PeriodicStartup,
)
from forze_socketio import (
    RealtimeConnection,
    refresh_presence,
    sweep_expired_connections,
)
from forze_socketio.connection import CONNECTION_SESSION_KEY

# ----------------------- #

_PRINCIPAL = UUID("22222222-2222-2222-2222-222222222222")
_TENANT = UUID("11111111-1111-1111-1111-111111111111")
_NOW = datetime(2026, 6, 22, 12, 0, 0, tzinfo=UTC)


def _conn(*, expires_at: datetime | None) -> RealtimeConnection:
    return RealtimeConnection(
        authn=AuthnIdentity(principal_id=_PRINCIPAL), tenant=_TENANT, expires_at=expires_at
    )


class _StubManager:
    def __init__(self, sids: list[str]) -> None:
        self._sids = sids

    def get_participants(self, namespace: str, room: str | None) -> Any:
        for sid in self._sids:
            yield sid, f"eio-{sid}"

    def is_connected(self, sid: str, namespace: str) -> bool:
        return True


class _StubSio:
    def __init__(self, sessions: dict[str, dict[str, Any]]) -> None:
        self.manager = _StubManager(list(sessions))
        self.sessions = sessions
        self.disconnected: list[str] = []

    async def get_session(self, sid: str, namespace: str | None = None) -> dict[str, Any]:
        return self.sessions.get(sid, {})

    async def disconnect(self, sid: str, namespace: str | None = None) -> None:
        self.disconnected.append(sid)


class _RecordingPresence:
    def __init__(self) -> None:
        self.joins: list[tuple[str, str]] = []

    async def joined(self, room: str, sid: str) -> None:
        self.joins.append((room, sid))

    async def left(self, room: str, sid: str) -> None: ...

    async def count(self, room: str) -> int:
        return 0


# ----------------------- #
# RealtimeConnection.is_expired


def test_is_expired() -> None:
    assert _conn(expires_at=None).is_expired(_NOW) is False
    assert _conn(expires_at=_NOW + timedelta(minutes=1)).is_expired(_NOW) is False
    assert _conn(expires_at=_NOW - timedelta(seconds=1)).is_expired(_NOW) is True
    assert _conn(expires_at=_NOW).is_expired(_NOW) is True  # boundary: at expiry


# ....................... #
# sweep_expired_connections


async def test_sweep_disconnects_only_expired() -> None:
    sessions = {
        "live": {CONNECTION_SESSION_KEY: _conn(expires_at=_NOW + timedelta(minutes=5))},
        "stale": {CONNECTION_SESSION_KEY: _conn(expires_at=_NOW - timedelta(minutes=5))},
        "eternal": {CONNECTION_SESSION_KEY: _conn(expires_at=None)},
        "anon": {},  # no connection stored (anonymous): never swept
    }
    sio = _StubSio(sessions)

    dropped = await sweep_expired_connections(cast(Any, sio), now=_NOW)

    assert dropped == 1
    assert sio.disconnected == ["stale"]


# ....................... #
# refresh_presence (heartbeat)


async def test_refresh_presence_reasserts_every_connection() -> None:
    sessions = {
        "sid-a": {CONNECTION_SESSION_KEY: _conn(expires_at=None)},
        "sid-b": {CONNECTION_SESSION_KEY: _conn(expires_at=None)},
        "anon": {},  # anonymous: nothing to refresh
    }
    sio = _StubSio(sessions)
    presence = _RecordingPresence()

    refreshed = await refresh_presence(cast(Any, sio), cast(Any, presence), namespace="/")

    room = _conn(expires_at=None).principal_room
    assert refreshed == 2
    assert presence.joins == [(room, "sid-a"), (room, "sid-b")]


# ....................... #
# periodic loop: ticks until cancelled, one bad tick does not kill it


async def test_periodic_startup_ticks_and_survives_errors() -> None:
    calls = {"n": 0}

    async def _tick() -> None:
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("boom")  # a bad tick must not stop the loop

    startup = _PeriodicStartup(tick=_tick, interval=timedelta(seconds=0.01), name="test")
    registered: list[object] = []
    ctx = cast(
        ExecutionContext,
        SimpleNamespace(drainables=SimpleNamespace(register=registered.append)),
    )

    await startup(ctx)
    for _ in range(200):
        await asyncio.sleep(0.005)
        if calls["n"] >= 3:
            break

    await _PeriodicShutdown(startup=startup)(ctx)

    assert calls["n"] >= 3  # survived the first failing tick and kept going
    # Drain-registered and stopped between ticks — cleanly, not cancelled mid-work.
    assert registered == [startup]
    task = startup.task
    assert task is not None and task.done() and not task.cancelled()


# ----------------------- #
# per-connection fault isolation — one bad socket must not shield the rest of a tick


class _FlakyDisconnectSio(_StubSio):
    """Disconnecting the named sid always raises (already gone, transport hiccup)."""

    def __init__(self, sessions: dict[str, dict[str, Any]], *, broken: str) -> None:
        super().__init__(sessions)
        self.broken = broken

    async def disconnect(self, sid: str, namespace: str | None = None) -> None:
        if sid == self.broken:
            raise RuntimeError("transport hiccup")

        await super().disconnect(sid, namespace=namespace)


class _FlakyPresence(_RecordingPresence):
    """Heartbeating the named room always raises (store blip for one key)."""

    def __init__(self, *, broken_sid: str, error: Exception | None = None) -> None:
        super().__init__()
        self.broken_sid = broken_sid
        self.error = error or RuntimeError("store blip")

    async def joined(self, room: str, sid: str) -> None:
        if sid == self.broken_sid:
            raise self.error

        await super().joined(room, sid)


async def test_sweep_continues_past_a_failing_disconnect() -> None:
    expired = _conn(expires_at=datetime(2020, 1, 1, tzinfo=UTC))
    sio = _FlakyDisconnectSio(
        {
            "sid-broken": {CONNECTION_SESSION_KEY: expired},
            "sid-later": {CONNECTION_SESSION_KEY: expired},
        },
        broken="sid-broken",
    )

    dropped = await sweep_expired_connections(cast(Any, sio))

    # the later expired socket was still dropped — one failure must not extend
    # everyone else's expired credentials to the next successful sweep
    assert sio.disconnected == ["sid-later"]
    assert dropped == 1


async def test_refresh_presence_continues_past_a_failing_heartbeat() -> None:
    live = _conn(expires_at=None)
    sio = _StubSio(
        {
            "sid-broken": {CONNECTION_SESSION_KEY: live},
            "sid-later": {CONNECTION_SESSION_KEY: live},
        }
    )
    presence = _FlakyPresence(broken_sid="sid-broken")

    refreshed = await refresh_presence(cast(Any, sio), presence)

    # the later connection still heartbeated — under a TTL store a persistently
    # failing early entry must not expire healthy connections behind it
    assert [sid for _room, sid in presence.joins] == ["sid-later"]
    assert refreshed == 1


async def test_a_key_error_from_the_presence_store_is_still_logged(
    error_log_buf: io.StringIO,
) -> None:
    live = _conn(expires_at=None)
    sio = _StubSio(
        {
            "sid-broken": {CONNECTION_SESSION_KEY: live},
            "sid-later": {CONNECTION_SESSION_KEY: live},
        }
    )
    presence = _FlakyPresence(broken_sid="sid-broken", error=KeyError("presence row gone"))

    refreshed = await refresh_presence(cast(Any, sio), presence)

    # Only a session that is gone is skipped quietly; a store's KeyError is a fault.
    assert [sid for _room, sid in presence.joins] == ["sid-later"]
    assert refreshed == 1
    (record,) = error_log_buf.getvalue().splitlines()
    assert "presence row gone" in record


# ----------------------- #
# a socket on its way out — over python-socketio's own server and manager

_EXPIRED = _conn(expires_at=_NOW - timedelta(minutes=5))


async def _listed(server: AsyncServer, eio_sid: str, connection: RealtimeConnection) -> str:
    """A ``/`` connection over its own Engine.IO socket, its session holding *connection*."""

    server.eio.sockets[eio_sid] = AsyncSocket(server.eio, eio_sid)
    sid: str = await server.manager.connect(eio_sid, "/")
    await server.save_session(sid, {CONNECTION_SESSION_KEY: connection})

    return sid


def _sids(server: AsyncServer) -> list[str]:
    return [sid for sid, _eio in server.manager.get_participants("/", None)]


async def test_the_sweep_skips_a_listed_socket_whose_session_is_gone(
    error_log_buf: io.StringIO,
) -> None:
    server = AsyncServer()
    leaving = await _listed(server, "eio-1", _EXPIRED)
    await _listed(server, "eio-2", _EXPIRED)
    server.eio.sockets.pop("eio-1")  # a close under way removed it before "/" let go

    dropped = await sweep_expired_connections(server, now=_NOW)

    assert dropped == 1
    assert _sids(server) == [leaving]  # left to its own disconnect handlers
    assert error_log_buf.getvalue() == ""


async def test_the_heartbeat_skips_a_listed_socket_whose_session_is_gone(
    error_log_buf: io.StringIO,
) -> None:
    server = AsyncServer()
    await _listed(server, "eio-1", _EXPIRED)
    staying = await _listed(server, "eio-2", _EXPIRED)
    server.eio.sockets.pop("eio-1")
    presence = _RecordingPresence()

    refreshed = await refresh_presence(server, cast(Any, presence))

    assert refreshed == 1
    assert presence.joins == [(_EXPIRED.principal_room, staying)]
    assert error_log_buf.getvalue() == ""


async def test_the_sweeps_leave_a_socket_whose_namespace_disconnect_has_begun(
    error_log_buf: io.StringIO,
) -> None:
    server = AsyncServer()
    leaving = await _listed(server, "eio-1", _EXPIRED)
    staying = await _listed(server, "eio-2", _EXPIRED)
    server.manager.pre_disconnect(leaving, "/")  # its disconnect handler is running
    presence = _RecordingPresence()

    refreshed = await refresh_presence(server, cast(Any, presence))
    dropped = await sweep_expired_connections(server, now=_NOW)

    # Not re-asserted beside the handler's leave, and not disconnected a second time.
    assert refreshed == 1
    assert presence.joins == [(_EXPIRED.principal_room, staying)]
    assert dropped == 1
    assert error_log_buf.getvalue() == ""


def test_naive_expires_at_is_refused_at_construction() -> None:
    # the sweep isolates per-connection failures, so a naive expires_at would not
    # error the connect — every expiry check would raise inside the sweep and be
    # logged-and-skipped, silently never enforcing; refuse it where it is resolved
    import pytest

    from forze.base.exceptions import CoreException

    with pytest.raises(CoreException) as caught:
        _conn(expires_at=datetime(2026, 1, 1))

    assert caught.value.code == "realtime_expiry_naive"
