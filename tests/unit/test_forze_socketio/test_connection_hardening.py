"""Connection hygiene — credential-expiry sweep + presence heartbeat (hardening)."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any, cast
from uuid import UUID

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

    def __init__(self, *, broken_sid: str) -> None:
        super().__init__()
        self.broken_sid = broken_sid

    async def joined(self, room: str, sid: str) -> None:
        if sid == self.broken_sid:
            raise RuntimeError("store blip")

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
