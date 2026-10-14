"""Under ``serve_uvicorn``, SIGTERM closes Socket.IO connections over the window first.

# covers: forze_socketio.drain (connection_drain_lifecycle_step under forze_fastapi.serve_uvicorn)

A real process: the app holds raw Engine.IO WebSockets open, receives SIGTERM, and must
close them spread over the drain window while it still serves — before uvicorn's own
shutdown, which would close every socket at once — and refuse connects meanwhile.
"""

from __future__ import annotations

import asyncio
import signal
import socket
import sys
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
import pytest
import websockets

# ----------------------- #

_APP = Path(__file__).with_name("_serve_drain_app.py")

WINDOW = 1.0
ANNOUNCE_DELAY = 0.5
SOCKETS = 5


def _free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))

        return probe.getsockname()[1]


def _events(record: Path) -> dict[str, float]:
    if not record.exists():
        return {}

    return {
        name: float(at) for name, at in (line.split() for line in record.read_text().splitlines())
    }


async def _readiness(port: int) -> int | None:
    try:
        async with httpx.AsyncClient() as client:
            return (await client.get(f"http://127.0.0.1:{port}/readyz", timeout=1.0)).status_code

    except httpx.HTTPError:
        return None


async def _wait_for(condition: str, check: object, *, within: float) -> None:
    deadline = time.monotonic() + within

    while time.monotonic() < deadline:
        if await check():  # type: ignore[operator]
            return

        await asyncio.sleep(0.02)

    raise AssertionError(f"timed out after {within}s waiting for {condition}")


@asynccontextmanager
async def _serving(
    tmp_path: Path,
) -> AsyncIterator[tuple[asyncio.subprocess.Process, int, Path]]:
    port = _free_port()
    record = tmp_path / "events"

    with (tmp_path / "stderr").open("wb") as stderr:
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            str(_APP),
            str(port),
            str(record),
            stdout=asyncio.subprocess.DEVNULL,
            stderr=stderr,
        )

        async def ready() -> bool:
            return await _readiness(port) == 200

        try:
            await _wait_for("readiness", ready, within=30.0)

            yield process, port, record

        except BaseException:
            sys.stderr.write((tmp_path / "stderr").read_text())
            raise

        finally:
            if process.returncode is None:
                process.kill()
                await asyncio.wait_for(process.wait(), timeout=10)


async def _connected(port: int) -> websockets.ClientConnection:
    ws = await websockets.connect(f"ws://127.0.0.1:{port}/socket.io/?EIO=4&transport=websocket")
    assert str(await asyncio.wait_for(ws.recv(), 2)).startswith("0")
    await ws.send("40")
    assert str(await asyncio.wait_for(ws.recv(), 2)).startswith("40")

    return ws


async def _closed_at(ws: websockets.ClientConnection) -> tuple[list[str], float]:
    frames: list[str] = []

    try:
        while True:
            frames.append(str(await ws.recv()))

    except websockets.ConnectionClosed:
        return frames, time.time()


# ----------------------- #


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX signals")
class TestDrainUnderServeUvicorn:
    async def test_sigterm_closes_connections_over_the_window_before_the_server_stops(
        self, tmp_path: Path
    ) -> None:
        async with _serving(tmp_path) as (process, port, record):
            sockets = [await _connected(port) for _ in range(SOCKETS)]
            closing = [asyncio.ensure_future(_closed_at(ws)) for ws in sockets]

            signalled = time.time()
            process.send_signal(signal.SIGTERM)

            async def not_ready() -> bool:
                return await _readiness(port) == 503

            await _wait_for("readiness to fail", not_ready, within=1.0)

            # While the replica drains, a connect is refused with the draining code.
            late = await websockets.connect(
                f"ws://127.0.0.1:{port}/socket.io/?EIO=4&transport=websocket"
            )
            await asyncio.wait_for(late.recv(), 2)
            await late.send("40")
            refusal = str(await asyncio.wait_for(late.recv(), 2))
            await late.close()

            outcomes = await asyncio.wait_for(asyncio.gather(*closing), timeout=10)
            code = await asyncio.wait_for(process.wait(), timeout=15)

        events = _events(record)
        closed = sorted(at for _frames, at in outcomes)

        assert code == 0
        assert refusal.startswith("44")
        assert '"code":"realtime_draining"' in refusal.replace(" ", "")
        # Each socket saw the Engine.IO CLOSE, then the transport closing.
        assert all(frames == ["1"] for frames, _at in outcomes)
        # After the announce delay, spread over the window rather than all at once, and
        # every one before the runtime's teardown — uvicorn's shutdown comes after.
        assert closed[0] - signalled >= ANNOUNCE_DELAY - 0.05
        assert closed[-1] - closed[0] >= WINDOW * (SOCKETS - 1) / SOCKETS - 0.3
        assert closed[-1] < events["teardown"] <= events["served"]
