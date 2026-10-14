"""What a client sees when the drain closes its connection — over a real server and socket.

# covers: forze_socketio.drain (close_spread against python-socketio/engineio under uvicorn)

socket.io's JavaScript client reconnects after a *transport* close and gives up after a
Socket.IO ``DISCONNECT`` packet ("io server disconnect"). Over a WebSocket it ignores the
Engine.IO ``CLOSE`` packet and notices only the socket closing. The raw client here follows
those rules, so it shows what that client would see. The Python client reconnects in
neither case; a test pins that, since the docs state it.

The reconnect tests put two servers behind a balancer that sends new connections to the
draining one until it notices: the client must ride its own backoff to the other. One runs
the real socket.io-client under node when ``FORZE_SOCKETIO_CLIENT_NODE_PATH`` names a
``node_modules`` that has it; the other emulates it, and always runs.
"""

from __future__ import annotations

import asyncio
import json
import os
import shutil
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from datetime import timedelta
from pathlib import Path

import httpx
import pytest
import socketio
import uvicorn
import websockets

from forze_socketio import (
    RealtimeConnectionStats,
    attach_realtime_connection,
    build_socketio_asgi_app,
    close_spread,
)

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


class _Polling:
    """A raw Engine.IO long-polling client that never leaves a poll pending between calls."""

    def __init__(self, client: httpx.AsyncClient, port: int) -> None:
        self.client = client
        self.url = f"http://127.0.0.1:{port}/socket.io/"
        self.sid = ""

    async def open(self) -> None:
        reply = await self.client.get(self.url, params={"EIO": "4", "transport": "polling"})
        self.sid = json.loads(reply.text[1:])["sid"]

    async def post(self, packet: str) -> httpx.Response:
        return await self.client.post(
            self.url, params={"EIO": "4", "transport": "polling", "sid": self.sid}, content=packet
        )

    async def poll(self) -> httpx.Response:
        return await self.client.get(
            self.url, params={"EIO": "4", "transport": "polling", "sid": self.sid}
        )


class TestPollingTransport:
    async def test_a_connect_turned_away_while_draining_does_not_hang_its_request(self) -> None:
        sio, stats, state = _replica()
        state["draining"] = True

        async with _serving(sio) as port, httpx.AsyncClient(timeout=3) as http:
            polling = _Polling(http, port)
            await polling.open()

            # No poll is pending: the turned-away connect must not wait for one.
            assert (await asyncio.wait_for(polling.post("40"), timeout=3)).status_code == 200
            after = await asyncio.wait_for(polling.poll(), timeout=3)

        assert stats.connect_refused == {("/", "draining"): 1}
        # Either way the client sees its transport end, and reconnects.
        assert after.status_code == 400 or after.text.startswith("1")

    # ....................... #

    async def test_the_drain_closes_a_polling_client_with_no_poll_pending(self) -> None:
        sio, _stats, _state = _replica()

        async with _serving(sio) as port, httpx.AsyncClient(timeout=3) as http:
            polling = _Polling(http, port)
            await polling.open()
            await polling.post("40")
            assert (await polling.poll()).text.startswith("40")  # connected; nothing pending

            closed = await asyncio.wait_for(
                close_spread(sio, _targets(sio), timedelta(seconds=0.05), tail=timedelta(1)),
                timeout=3,
            )

        assert closed == 1  # finished, not left waiting for a poll that never comes


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


# ----------------------- #


class _Balancer:
    """Sends each new TCP connection to the current upstream — the draining replica until
    the test says the balancer noticed."""

    def __init__(self, upstream: int) -> None:
        self.upstream = upstream

    async def _pipe(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            while data := await reader.read(65536):
                writer.write(data)
                await writer.drain()

        except ConnectionError:
            pass

        finally:
            writer.close()

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        up_reader, up_writer = await asyncio.open_connection("127.0.0.1", self.upstream)
        await asyncio.gather(self._pipe(reader, up_writer), self._pipe(up_reader, writer))

    @asynccontextmanager
    async def serving(self) -> AsyncIterator[int]:
        server = await asyncio.start_server(self._handle, "127.0.0.1", 0)

        try:
            yield server.sockets[0].getsockname()[1]

        finally:
            server.close()


def _replica() -> tuple[socketio.AsyncServer, RealtimeConnectionStats, dict[str, bool]]:
    sio = socketio.AsyncServer(async_mode="asgi")
    stats = RealtimeConnectionStats()
    state = {"draining": False}
    attach_realtime_connection(
        sio, resolve=lambda _connect: None, draining=lambda: state["draining"], stats=stats
    )

    return sio, stats, state


async def _until(check: Callable[[], bool], *, within: float) -> None:
    for _ in range(int(within / 0.02)):  # bounded
        if check():
            return

        await asyncio.sleep(0.02)

    raise AssertionError(f"not reached within {within}s")


async def _drain_into_the_other_replica(
    client: Callable[[int], Awaitable[None]],
) -> tuple[RealtimeConnectionStats, RealtimeConnectionStats]:
    """Connect through the balancer to A, drain A while the balancer still sends reconnects
    there, then let the balancer notice; return A's and B's counts."""

    leaving, leaving_stats, leaving_state = _replica()
    staying, staying_stats, _ = _replica()

    async with _serving(leaving) as port_a, _serving(staying) as port_b:
        balancer = _Balancer(port_a)

        async with balancer.serving() as port:
            connecting = asyncio.ensure_future(client(port))

            try:
                await _until(lambda: leaving_stats.connections.get("/") == 1, within=5)

                leaving_state["draining"] = True
                await close_spread(
                    leaving, _targets(leaving), timedelta(seconds=0.05), tail=timedelta(1)
                )
                # The reconnect lands back on the draining replica, which turns it away...
                await _until(
                    lambda: leaving_stats.connect_refused.get(("/", "draining"), 0) >= 1,
                    within=5,
                )
                # ...and once the balancer notices, the client's own backoff carries it over.
                balancer.upstream = port_b
                await _until(lambda: staying_stats.connections.get("/") == 1, within=5)

            finally:
                connecting.cancel()
                await asyncio.gather(connecting, return_exceptions=True)

    return leaving_stats, staying_stats


class TestReconnectAfterDrain:
    async def test_a_client_following_the_javascript_rules_lands_on_the_other_replica(
        self,
    ) -> None:
        outcomes: list[str] = []

        async def client(port: int) -> None:
            """socket.io-client's rules over a WebSocket: reconnect with backoff after a
            transport close; give up for good on CONNECT_ERROR or a namespace DISCONNECT."""

            url = f"ws://127.0.0.1:{port}/socket.io/?EIO=4&transport=websocket"

            while True:
                try:
                    async with websockets.connect(url) as ws:
                        await asyncio.wait_for(ws.recv(), 2)  # Engine.IO OPEN
                        await ws.send("40")

                        async for frame in ws:
                            if str(frame).startswith(("44", "41")):
                                outcomes.append("gave up")
                                return

                            if str(frame).startswith("40"):
                                outcomes.append("connect")

                except (websockets.ConnectionClosed, OSError):
                    pass

                outcomes.append("transport close")
                await asyncio.sleep(0.2)

        leaving, staying = await _drain_into_the_other_replica(client)

        # Connected to A, turned away there by transport closes, then connected to B.
        assert "gave up" not in outcomes
        assert outcomes.count("connect") == 2
        assert leaving.connect_refused[("/", "draining")] >= 1
        assert staying.connect_refused == {}

    # ....................... #

    @pytest.mark.skipif(
        shutil.which("node") is None or not os.environ.get("FORZE_SOCKETIO_CLIENT_NODE_PATH"),
        reason="needs node and FORZE_SOCKETIO_CLIENT_NODE_PATH (a node_modules with socket.io-client)",
    )
    async def test_the_real_javascript_client_lands_on_the_other_replica(self) -> None:
        events: list[str] = []
        script = Path(__file__).with_name("_socketio_client.js")

        async def client(port: int) -> None:
            node = await asyncio.create_subprocess_exec(
                "node",
                str(script),
                f"http://127.0.0.1:{port}",
                env={**os.environ, "NODE_PATH": os.environ["FORZE_SOCKETIO_CLIENT_NODE_PATH"]},
                stdout=asyncio.subprocess.PIPE,
            )

            try:
                while line := await node.stdout.readline():  # type: ignore[union-attr]
                    events.append(line.decode().strip())

            finally:
                if node.returncode is None:
                    node.kill()
                    await node.wait()

        leaving, _staying = await _drain_into_the_other_replica(client)

        assert "connect_error active=false" not in events
        assert events.count("connect") == 2
        assert "disconnect transport close" in events
        assert leaving.connect_refused[("/", "draining")] >= 1
