"""The connection lifecycle binds the connection's authn + tenant for replay and ack.

Replay and ack run on the serving runtime's live context, from a socket.io handler's task
where nothing is bound; the old `_bind_tenant` read the authn from that context
(`ctx.inv_ctx.get_authn()` → ``None``), losing the authenticated identity for mailbox/cursor
operations. These also exercise the handlers directly — the point of extracting
`_ConnectionLifecycle` from the closures it replaced.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any
from uuid import UUID

import pytest

from forze.application.contracts.authn import AuthnIdentity
from forze.application.execution import DepsRegistry, ExecutionContext, ExecutionRuntime
from forze.base.exceptions import CoreException
from forze.base.primitives import HlcTimestamp
from forze_mock import MockDepsModule
from forze_socketio import RealtimeConnection
from forze_socketio.connection import CONNECTION_SESSION_KEY, _ConnectionLifecycle

# ----------------------- #

_AUTHN = AuthnIdentity(principal_id=UUID("33333333-3333-3333-3333-333333333333"))
_TENANT = UUID("11111111-1111-1111-1111-111111111111")


class _StubSio:
    manager = SimpleNamespace(
        is_connected=lambda _sid, _namespace: True, eio_sid_from_sid=lambda _sid, _namespace: None
    )
    """Every sid stays connected, on no Engine.IO connection the stub models: the connect
    checks before each stage that it still is, and records what it accepted on it."""

    def __init__(self) -> None:
        self.sessions: dict[str, dict[str, Any]] = {}
        self.rooms: list[str] = []
        self.emits: list[Any] = []

    async def get_session(self, sid: str, namespace: str | None = None) -> dict[str, Any]:
        return self.sessions.setdefault(sid, {})

    async def save_session(self, sid: str, session: dict[str, Any], namespace: str | None = None) -> None:
        self.sessions[sid] = session

    async def enter_room(self, sid: str, room: str, namespace: str | None = None) -> None:
        self.rooms.append(room)

    async def emit(self, event: str, data: Any = None, **_: Any) -> None:
        self.emits.append({"event": event, "data": data})


class _RecordingMailbox:
    """Captures the ambient identity at the moment a mailbox op runs."""

    def __init__(self, ctx: ExecutionContext, seen: dict[str, Any]) -> None:
        self._ctx, self._seen = ctx, seen

    def _capture(self) -> None:
        self._seen["authn"] = self._ctx.inv_ctx.get_authn()
        self._seen["tenant"] = self._ctx.inv_ctx.get_tenant()

    async def read_since(self, *, principal: str, since: Any) -> list[Any]:
        self._capture()
        return []

    async def position_of(self, *, principal: str, event_id: str) -> HlcTimestamp:
        self._capture()
        return HlcTimestamp(physical_ms=5, logical=0)

    async def trim(self, *, principal: str, before: HlcTimestamp) -> None:
        self._capture()


class _RecordingCursors:
    def __init__(self, ctx: ExecutionContext, seen: dict[str, Any]) -> None:
        self._ctx, self._seen = ctx, seen

    async def get(self, *, principal: str, client_key: str) -> None:
        return None

    async def advance(self, *, principal: str, client_key: str, up_to: HlcTimestamp) -> None:
        self._seen["advanced"] = up_to

    async def min_cursor(self, *, principal: str) -> None:
        return None


def _lifecycle(sio: _StubSio, seen: dict[str, Any], runtime: ExecutionRuntime,
               connection: RealtimeConnection) -> _ConnectionLifecycle:
    return _ConnectionLifecycle(
        sio=sio,  # type: ignore[arg-type]
        namespace="/",
        resolve=lambda _c: connection,
        mailbox_factory=lambda ctx: _RecordingMailbox(ctx, seen),  # type: ignore[arg-type,return-value]
        cursors_factory=lambda ctx: _RecordingCursors(ctx, seen),  # type: ignore[arg-type,return-value]
        runtime=runtime,
    )


# ----------------------- #


async def test_ack_runs_under_the_connection_identity(serving_runtime: ExecutionRuntime) -> None:
    sio, seen, runtime = _StubSio(), {}, serving_runtime
    connection = RealtimeConnection(authn=_AUTHN, tenant=_TENANT)
    lifecycle = _lifecycle(sio, seen, runtime, connection)
    sio.sessions["s1"] = {CONNECTION_SESSION_KEY: connection}  # as connect would have stored

    await lifecycle.on_ack("s1", {"up_to": str(UUID(int=1))})

    # the ack ran under the connection's authenticated identity — not the handler task's
    # empty one (the bug bound authn=None), and under its tenant
    assert seen["authn"] == _AUTHN
    assert seen["tenant"].tenant_id == _TENANT
    assert seen.get("advanced") is not None  # the cursor advanced


async def test_replay_runs_under_the_connection_identity(
    serving_runtime: ExecutionRuntime,
) -> None:
    sio, seen, runtime = _StubSio(), {}, serving_runtime
    connection = RealtimeConnection(authn=_AUTHN, tenant=_TENANT)

    await _lifecycle(sio, seen, runtime, connection).replay(connection, "s1")

    assert seen["authn"] == _AUTHN
    assert seen["tenant"].tenant_id == _TENANT


async def test_connect_stores_identity_and_joins_principal_room(
    serving_runtime: ExecutionRuntime,
) -> None:
    sio, seen, runtime = _StubSio(), {}, serving_runtime
    connection = RealtimeConnection(authn=_AUTHN, tenant=_TENANT)

    await _lifecycle(sio, seen, runtime, connection).on_connect("s1", {}, {"token": "x"})

    assert connection.principal_room in sio.rooms
    assert sio.sessions["s1"][CONNECTION_SESSION_KEY] is connection
    assert seen["authn"] == _AUTHN  # the on-connect replay also bound the identity


async def test_connections_replaying_at_once_each_run_under_their_own_identity(
    serving_runtime: ExecutionRuntime,
) -> None:
    # Every connection replays on the one serving context; the identity each binds is its
    # handler task's own, so none sees another's while they interleave.
    seen: dict[str, Any] = {}

    class _Interleaving(_RecordingMailbox):
        async def read_since(self, *, principal: str, since: Any) -> list[Any]:
            await asyncio.sleep(0.01)  # the other replay binds its identity meanwhile
            self._seen[principal] = (self._ctx.inv_ctx.get_authn(), self._ctx.inv_ctx.get_tenant())
            return []

    connections = [
        RealtimeConnection(authn=AuthnIdentity(principal_id=UUID(int=n)), tenant=UUID(int=100 + n))
        for n in (1, 2)
    ]
    sio = _StubSio()
    lifecycles = [
        _ConnectionLifecycle(
            sio=sio,  # type: ignore[arg-type]
            namespace="/",
            resolve=lambda _c, c=connection: c,
            mailbox_factory=lambda ctx: _Interleaving(ctx, seen),  # type: ignore[arg-type,return-value]
            cursors_factory=lambda ctx: _RecordingCursors(ctx, {}),  # type: ignore[arg-type,return-value]
            runtime=serving_runtime,
        )
        for connection in connections
    ]

    await asyncio.gather(
        *(
            lifecycle.replay(connection, f"s{n}")
            for n, (lifecycle, connection) in enumerate(zip(lifecycles, connections, strict=True))
        )
    )

    for connection in connections:
        authn, tenant = seen[connection.principal]
        assert authn == connection.authn
        assert tenant.tenant_id == connection.tenant


async def test_replay_and_ack_need_the_serving_scope_open() -> None:
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(MockDepsModule()).freeze())
    connection = RealtimeConnection(authn=_AUTHN, tenant=_TENANT)
    sio, seen = _StubSio(), {}
    sio.sessions["s1"] = {CONNECTION_SESSION_KEY: connection}
    lifecycle = _lifecycle(sio, seen, runtime, connection)

    with pytest.raises(CoreException) as replaying:
        await lifecycle.replay(connection, "s1")

    with pytest.raises(CoreException) as acking:
        await lifecycle.on_ack("s1", {"up_to": str(UUID(int=1))})

    assert replaying.value.code == acking.value.code == "realtime_replay_scope_closed"
