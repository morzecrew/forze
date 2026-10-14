"""The admission hook: an app refuses or delays a connect before it is authenticated.

# covers: forze_socketio.connection (attach_realtime_connection admission=)
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from types import SimpleNamespace
from typing import Any
from uuid import UUID

import pytest
from socketio.async_server import AsyncServer
from socketio.exceptions import ConnectionRefusedError as SocketIOConnectionRefusedError

from forze.application.contracts.authn import AuthnIdentity
from forze.application.execution import build_runtime
from forze.base.exceptions import GENERIC_INTERNAL_DETAIL, exc
from forze_socketio import (
    RealtimeConnection,
    RealtimeConnectionStats,
    SocketIOConnect,
    attach_realtime_connection,
)

# ----------------------- #

_PRINCIPAL = UUID("33333333-3333-3333-3333-333333333333")


class _Recorder:
    """Records the order the connect's stages ran in."""

    def __init__(self) -> None:
        self.calls: list[str] = []

    async def resolve(self, connect: SocketIOConnect) -> RealtimeConnection:
        self.calls.append("resolve")
        return RealtimeConnection(authn=AuthnIdentity(principal_id=_PRINCIPAL))


def _attach(
    server: AsyncServer, recorder: _Recorder, admission: Any, **options: Any
) -> RealtimeConnectionStats:
    stats = RealtimeConnectionStats()
    attach_realtime_connection(
        server, resolve=recorder.resolve, admission=admission, stats=stats, **options
    )

    return stats


async def _connect(server: AsyncServer, auth: Any = None) -> str:
    server.eio.sockets["eio-1"] = SimpleNamespace(session={}, closed=False)
    sid = await server.manager.connect("eio-1", "/")
    await server.handlers["/"]["connect"](sid, {"REMOTE_ADDR": "10.0.0.7"}, auth)

    return sid


# ----------------------- #


class TestAdmission:
    async def test_it_runs_before_authentication_with_the_connect(self) -> None:
        server, recorder = AsyncServer(), _Recorder()
        seen: list[SocketIOConnect] = []

        async def admission(connect: SocketIOConnect) -> None:
            recorder.calls.append("admission")
            seen.append(connect)

        stats = _attach(server, recorder, admission)
        sid = await _connect(server, {"token": "t"})

        assert recorder.calls == ["admission", "resolve"]
        [connect] = seen
        assert (connect.sid, connect.namespace, connect.auth) == (sid, "/", {"token": "t"})
        assert connect.environ["REMOTE_ADDR"] == "10.0.0.7"
        assert stats.connections == {"/": 1}

    # ....................... #

    async def test_a_client_safe_refusal_carries_its_summary(self) -> None:
        server, recorder = AsyncServer(), _Recorder()

        async def admission(_connect: SocketIOConnect) -> None:
            raise exc.throttled("Too many connects, try again shortly")

        stats = _attach(server, recorder, admission)

        with pytest.raises(SocketIOConnectionRefusedError) as caught:
            # An unsupported protocol too: admission runs before negotiation.
            await _connect(server, {"protocol": 99})

        assert caught.value.error_args == {"message": "Too many connects, try again shortly"}
        assert recorder.calls == []
        assert stats.connect_refused == {("/", "admission"): 1}
        assert stats.connections == {}

    # ....................... #

    async def test_a_server_side_refusal_shows_only_the_generic_detail(self) -> None:
        server, recorder = AsyncServer(), _Recorder()

        async def admission(_connect: SocketIOConnect) -> None:
            raise exc.infrastructure("admission store unreachable at 10.0.0.9")

        stats = _attach(server, recorder, admission)

        with pytest.raises(SocketIOConnectionRefusedError) as caught:
            await _connect(server)

        assert caught.value.error_args == {"message": GENERIC_INTERNAL_DETAIL}
        assert stats.connect_refused == {("/", "admission"): 1}

    # ....................... #

    async def test_an_unexpected_error_is_refused_as_internal(self) -> None:
        server, recorder = AsyncServer(), _Recorder()

        async def admission(_connect: SocketIOConnect) -> None:
            raise RuntimeError("hook bug")

        stats = _attach(server, recorder, admission)

        with pytest.raises(SocketIOConnectionRefusedError) as caught:
            await _connect(server)

        assert caught.value.error_args == {"message": GENERIC_INTERNAL_DETAIL}
        assert stats.connect_refused == {("/", "internal"): 1}

    # ....................... #

    async def test_awaiting_it_delays_the_connect(self) -> None:
        server, recorder = AsyncServer(), _Recorder()
        admitted = asyncio.Event()

        async def admission(_connect: SocketIOConnect) -> None:
            await admitted.wait()

        _attach(server, recorder, admission)
        connecting = asyncio.ensure_future(_connect(server))
        await asyncio.sleep(0.05)

        assert not connecting.done()
        assert recorder.calls == []

        admitted.set()
        await asyncio.wait_for(connecting, timeout=1.0)

        assert recorder.calls == ["resolve"]

    # ....................... #

    async def test_a_draining_replica_refuses_before_admission(self) -> None:
        server, recorder = AsyncServer(), _Recorder()
        runtime = build_runtime(drain_timeout=timedelta(0))

        async def admission(_connect: SocketIOConnect) -> None:
            recorder.calls.append("admission")

        stats = _attach(server, recorder, admission, draining=lambda: runtime.draining)

        async with runtime.scope():
            runtime.begin_draining()

            with pytest.raises(SocketIOConnectionRefusedError):
                await _connect(server)

        assert recorder.calls == []
        assert stats.connect_refused == {("/", "draining"): 1}

    # ....................... #

    async def test_its_own_refusal_reaches_the_client_as_raised(self) -> None:
        server, recorder = AsyncServer(), _Recorder()

        async def admission(_connect: SocketIOConnect) -> None:
            raise SocketIOConnectionRefusedError("Busy", {"retry_after": 3})

        stats = _attach(server, recorder, admission)

        with pytest.raises(SocketIOConnectionRefusedError) as caught:
            await _connect(server)

        assert caught.value.error_args == {"message": "Busy", "data": {"retry_after": 3}}
        assert stats.connect_refused == {("/", "admission"): 1}
