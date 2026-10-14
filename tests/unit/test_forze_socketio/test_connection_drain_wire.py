"""What a client sees when the drain closes its connection — over a real server and socket.

# covers: forze_socketio.drain (close_spread against python-socketio/engineio under uvicorn)

socket.io's JavaScript client reconnects after a *transport* close and gives up after a
Socket.IO ``DISCONNECT`` packet ("io server disconnect"). Over a WebSocket it ignores the
Engine.IO ``CLOSE`` packet and notices only the socket closing. The raw client here follows
those rules, so it shows what that client would see. The Python client reconnects in
neither case; the last test pins that, since the docs state it.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import timedelta

import socketio
import uvicorn
import websockets

from forze_socketio import build_socketio_asgi_app, close_spread

# ----------------------- #


@asynccontextmanager
async def _serving(sio: socketio.AsyncServer) -> AsyncIterator[int]:
    server = uvicorn.Server(
        uvicorn.Config(build_socketio_asgi_app(sio), host="127.0.0.1", port=0, log_level="error")
    )
    serving = asyncio.ensure_future(server.serve())

    try:
        for _ in range(500):  # bounded: 5 s
            if server.started:
                break

            await asyncio.sleep(0.01)

        yield server.servers[0].sockets[0].getsockname()[1]

    finally:
        server.should_exit = True
        await asyncio.wait_for(serving, timeout=10)


async def _open_socket(port: int) -> websockets.ClientConnection:
    """A raw Engine.IO WebSocket with namespace ``/`` connected."""

    ws = await websockets.connect(f"ws://127.0.0.1:{port}/socket.io/?EIO=4&transport=websocket")
    assert (await asyncio.wait_for(ws.recv(), 2))[0] == "0"  # Engine.IO OPEN
    await ws.send("40")
    assert str(await asyncio.wait_for(ws.recv(), 2)).startswith("40")  # Socket.IO CONNECT

    return ws


async def _frames_until_closed(ws: websockets.ClientConnection, *, within: float) -> list[str]:
    frames: list[str] = []

    async def read() -> None:
        try:
            while True:
                frames.append(str(await ws.recv()))

        except websockets.ConnectionClosed:
            return

    await asyncio.wait_for(read(), timeout=within)

    return frames


def _targets(sio: socketio.AsyncServer) -> list[tuple[str, str]]:
    return [("/", eio_sid) for _sid, eio_sid in sio.manager.get_participants("/", None)]


# ----------------------- #


class TestTransportClose:
    async def test_the_drain_closes_the_transport_without_a_namespace_disconnect(self) -> None:
        sio = socketio.AsyncServer(async_mode="asgi")

        async with _serving(sio) as port:
            sockets = [await _open_socket(port) for _ in range(3)]

            closed = await close_spread(
                sio, _targets(sio), timedelta(seconds=0.2), tail=timedelta(seconds=1)
            )

            # Closed within the drain, not at the client's ping timeout 45 s later.
            frames = [await _frames_until_closed(ws, within=2.0) for ws in sockets]

        assert closed == 3
        # The Engine.IO CLOSE, then the socket closing: a transport close. Never "41", the
        # Socket.IO DISCONNECT the JavaScript client reports as "io server disconnect".
        assert frames == [["1"], ["1"], ["1"]]

    # ....................... #

    async def test_a_namespace_disconnect_is_what_the_drain_avoids(self) -> None:
        sio = socketio.AsyncServer(async_mode="asgi")

        async with _serving(sio) as port:
            ws = await _open_socket(port)
            [(sid, _eio_sid)] = list(sio.manager.get_participants("/", None))

            await sio.disconnect(sid)

            assert await asyncio.wait_for(ws.recv(), 2) == "41"
            await ws.close()


# ----------------------- #


class TestPythonClient:
    async def test_the_python_client_does_not_reconnect_after_the_drain(self) -> None:
        sio = socketio.AsyncServer(async_mode="asgi")
        connects: list[str] = []
        reasons: list[str] = []

        @sio.on("connect")
        async def on_connect(sid: str, _environ: object, _auth: object = None) -> None:
            connects.append(sid)

        client = socketio.AsyncClient(reconnection_delay=0.05, reconnection_delay_max=0.1)

        @client.on("disconnect")
        async def on_disconnect(reason: str = "") -> None:
            reasons.append(reason)

        async with _serving(sio) as port:
            await client.connect(f"http://127.0.0.1:{port}", transports=["websocket"])
            await close_spread(sio, _targets(sio), timedelta(seconds=0.05), tail=timedelta(1))

            await asyncio.sleep(0.5)  # ten reconnect delays: time enough to have tried

            assert client.connected is False
            assert len(connects) == 1
            assert reasons == ["server disconnect"]
