"""``serve_uvicorn`` routes SIGTERM into announce → drain phases → exit, in a real process."""

from __future__ import annotations

import asyncio
import signal
import socket
import subprocess
import sys
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from datetime import timedelta
from pathlib import Path

import httpx
import pytest
import uvicorn
from fastapi import FastAPI

from forze.application.execution import build_runtime
from forze.base.exceptions import CoreException, ExceptionKind
from forze_fastapi import serve_uvicorn
from forze_fastapi.serving import _DrainingServer  # pyright: ignore[reportPrivateUsage]

# ----------------------- #

_APP = Path(__file__).with_name("_serve_uvicorn_app.py")

ANNOUNCE_DELAY = 1.5
GRACEFUL_TIMEOUT = 1.0


def _free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))

        return probe.getsockname()[1]


def _events(record: Path) -> list[str]:
    return record.read_text().split() if record.exists() else []


def _wait_for(condition: str, check: object, *, within: float) -> None:
    deadline = time.monotonic() + within

    while time.monotonic() < deadline:
        if check():  # type: ignore[operator]
            return

        time.sleep(0.02)

    raise AssertionError(f"timed out after {within}s waiting for {condition}")


def _readiness(port: int) -> int | None:
    try:
        return httpx.get(f"http://127.0.0.1:{port}/readyz", timeout=1.0).status_code

    except httpx.HTTPError:
        return None


@contextmanager
def _serving(tmp_path: Path, mode: str) -> Iterator[tuple[subprocess.Popen[bytes], int, Path]]:
    port = _free_port()
    record = tmp_path / "events"

    with (tmp_path / "stderr").open("wb") as stderr:
        process = subprocess.Popen(
            [sys.executable, str(_APP), str(port), str(record), mode],
            stdout=subprocess.DEVNULL,
            stderr=stderr,
        )

        try:
            _wait_for("readiness", lambda: _readiness(port) == 200, within=30.0)

            yield process, port, record

        except BaseException:
            sys.stderr.write((tmp_path / "stderr").read_text())
            raise

        finally:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=10)


@contextmanager
def _open_stream(port: int) -> Iterator[socket.socket]:
    """An SSE response in progress, its client reading nothing more."""

    client = socket.create_connection(("127.0.0.1", port), timeout=5.0)

    try:
        client.sendall(b"GET /stream HTTP/1.1\r\nHost: test\r\n\r\n")
        assert b"200" in client.recv(1024)

        yield client

    finally:
        with suppress(OSError):
            client.close()


# ----------------------- #


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX signals")
class TestServeUvicorn:
    def test_sigterm_withdraws_readiness_drains_then_exits_cleanly(self, tmp_path: Path) -> None:
        with _serving(tmp_path, "quick") as (process, port, record):
            with _open_stream(port):
                started = time.monotonic()
                process.send_signal(signal.SIGTERM)

                # Still serving, but no longer ready: the balancer's cue to stop routing.
                _wait_for("readiness to fail", lambda: _readiness(port) == 503, within=1.0)

                code = process.wait(timeout=ANNOUNCE_DELAY + GRACEFUL_TIMEOUT + 5.0)
                elapsed = time.monotonic() - started

        assert code == 0
        assert _events(record) == ["phase-started", "phase-done", "teardown", "served"]
        # The open stream held the exit only for the graceful timeout, not until killed.
        assert ANNOUNCE_DELAY <= elapsed < ANNOUNCE_DELAY + GRACEFUL_TIMEOUT + 3.0

    # ....................... #

    def test_a_second_signal_cancels_a_slow_phase_and_exits_at_once(self, tmp_path: Path) -> None:
        with _serving(tmp_path, "slow") as (process, _port, record):
            process.send_signal(signal.SIGINT)
            _wait_for(
                "the slow phase to start",
                lambda: "phase-started" in _events(record),
                within=ANNOUNCE_DELAY + 5.0,
            )

            started = time.monotonic()
            process.send_signal(signal.SIGINT)
            code = process.wait(timeout=10.0)
            elapsed = time.monotonic() - started

        assert code == 0
        assert _events(record)[:2] == ["phase-started", "phase-cancelled"]
        assert "phase-done" not in _events(record)
        # Uvicorn skips the lifespan shutdown on a forced exit; the scope still unwinds
        # when asyncio.run cancels the lifespan task.
        assert _events(record)[-2:] == ["served", "teardown"]
        assert elapsed < 3.0

    # ....................... #

    def test_a_drain_that_raises_still_lets_the_server_exit(self, tmp_path: Path) -> None:
        with _serving(tmp_path, "raise") as (process, _port, record):
            process.send_signal(signal.SIGTERM)
            code = process.wait(timeout=10.0)

        assert code == 0
        assert _events(record)[0] == "drain-raised"
        assert _events(record)[-2:] == ["teardown", "served"]


# ----------------------- #


def _server() -> _DrainingServer:
    return _DrainingServer(
        uvicorn.Config(FastAPI()),
        build_runtime(),
        drain_budget=timedelta(seconds=1),
        announce_delay=timedelta(0),
    )


class TestSignalCapture:
    """The in-process half: which handlers are installed, and what is put back."""

    @pytest.mark.asyncio
    async def test_the_previous_handlers_are_restored_on_the_way_out(self) -> None:
        before = {sig: signal.getsignal(sig) for sig in (signal.SIGINT, signal.SIGTERM)}
        server = _server()

        with server.capture_signals():
            assert signal.getsignal(signal.SIGTERM) == server._on_signal  # pyright: ignore[reportPrivateUsage]

        assert {sig: signal.getsignal(sig) for sig in before} == before

    # ....................... #

    def test_off_the_main_thread_no_handler_is_installed(self) -> None:
        before = signal.getsignal(signal.SIGTERM)
        seen: list[object] = []

        async def inside() -> None:
            with _server().capture_signals():
                seen.append(signal.getsignal(signal.SIGTERM))

        worker = threading.Thread(target=lambda: asyncio.run(inside()))
        worker.start()
        worker.join(timeout=10.0)

        assert seen == [before]

    # ....................... #

    @pytest.mark.asyncio
    async def test_a_uvicorn_without_capture_signals_is_refused(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.delattr(uvicorn.Server, "capture_signals")

        with pytest.raises(CoreException) as caught:
            await serve_uvicorn(
                uvicorn.Config(FastAPI()), build_runtime(), drain_budget=timedelta(seconds=1)
            )

        assert caught.value.kind == ExceptionKind.CONFIGURATION

    # ....................... #

    @pytest.mark.asyncio
    @pytest.mark.parametrize(("configured", "effective"), [(None, 3), (7, 7)])
    async def test_a_graceful_timeout_is_set_only_when_unset(
        self,
        monkeypatch: pytest.MonkeyPatch,
        configured: int | None,
        effective: int,
    ) -> None:
        async def no_serve(_self: object, sockets: object = None) -> None:
            return None

        monkeypatch.setattr(_DrainingServer, "serve", no_serve)
        config = uvicorn.Config(FastAPI(), timeout_graceful_shutdown=configured)

        await serve_uvicorn(
            config,
            build_runtime(),
            drain_budget=timedelta(seconds=1),
            graceful_timeout=timedelta(seconds=2.5),
        )

        assert config.timeout_graceful_shutdown == effective
