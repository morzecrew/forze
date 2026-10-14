"""``serve_uvicorn`` routes SIGTERM into announce → drain phases → exit, in a real process."""

from __future__ import annotations

import asyncio
import contextvars
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
from typing import cast

import httpx
import pytest
import uvicorn
from fastapi import FastAPI

from forze.application.contracts.execution import LifecycleStep
from forze.application.execution import ExecutionContext, build_runtime
from forze.application.execution.runtime import ExecutionRuntime
from forze.base.exceptions import CoreException, ExceptionKind
from forze_fastapi import runtime_lifespan, serve_uvicorn
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
        with _serving(tmp_path, "quick") as (process, port, record), _open_stream(port):
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


_REQUEST: contextvars.ContextVar[str | None] = contextvars.ContextVar("request", default=None)
"""Stands in for whatever a request handler binds: a tenant, an identity, a deadline."""


class _FakeRuntime:
    """Records the context its drain runs in; the drain lasts until released."""

    drain_timeout = timedelta(0)
    shutdown_step_timeout = timedelta(0)

    def __init__(self) -> None:
        self.seen: list[str | None] = []
        self.release = asyncio.Event()
        self.cancelled = False

    async def drain(self, _budget: timedelta, *, announce_delay: timedelta) -> None:
        self.seen.append(_REQUEST.get())

        try:
            await asyncio.wait_for(self.release.wait(), timeout=10.0)

        except asyncio.CancelledError:
            self.cancelled = True
            raise


def _server(runtime: object | None = None) -> _DrainingServer:
    return _DrainingServer(
        uvicorn.Config(FastAPI()),
        build_runtime() if runtime is None else cast(ExecutionRuntime, runtime),
        drain_budget=timedelta(seconds=1),
        announce_delay=timedelta(0),
    )


async def _settled(server: _DrainingServer) -> None:
    """Let the loop run the scheduled signal callback, bounded."""

    for _ in range(100):
        await asyncio.sleep(0.01)

        if server._drain_task is not None:  # pyright: ignore[reportPrivateUsage]
            return


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

    # ....................... #

    @pytest.mark.asyncio
    async def test_the_drain_runs_in_the_servers_context_not_the_interrupted_tasks(
        self,
    ) -> None:
        """A signal handler runs inside whatever frame it interrupted — often a request's."""

        runtime = _FakeRuntime()
        runtime.release.set()
        server = _server(runtime)

        async def request() -> None:
            _REQUEST.set("request-1")
            # The signal lands here, mid-request: the handler runs in this frame.
            server._on_signal(signal.SIGTERM, None)  # pyright: ignore[reportPrivateUsage]

        with server.capture_signals():
            await asyncio.create_task(request())
            await _settled(server)
            await asyncio.wait_for(server._drain_task, timeout=5.0)  # type: ignore[arg-type]  # pyright: ignore[reportPrivateUsage]

        assert runtime.seen == [None]

    # ....................... #

    @pytest.mark.asyncio
    async def test_a_repeated_sigterm_does_not_cut_the_drain(self) -> None:
        """A SIGTERM can arrive twice: to the process group, and again from a wrapper."""

        runtime = _FakeRuntime()
        server = _server(runtime)

        with server.capture_signals():
            server._on_signal(signal.SIGTERM, None)  # pyright: ignore[reportPrivateUsage]
            await _settled(server)
            server._on_signal(signal.SIGTERM, None)  # pyright: ignore[reportPrivateUsage]
            await asyncio.sleep(0.05)

            drain = server._drain_task  # pyright: ignore[reportPrivateUsage]
            assert drain is not None and not drain.done()
            assert (server.should_exit, server.force_exit, runtime.cancelled) == (
                False,
                False,
                False,
            )

            runtime.release.set()
            await asyncio.wait_for(drain, timeout=5.0)

        assert (server.should_exit, server.force_exit) == (True, False)

    # ....................... #

    @pytest.mark.asyncio
    @pytest.mark.parametrize("first", [signal.SIGTERM, signal.SIGINT])
    async def test_a_sigint_during_the_drain_forces_the_exit(self, first: int) -> None:
        runtime = _FakeRuntime()
        server = _server(runtime)

        with server.capture_signals():
            server._on_signal(first, None)  # pyright: ignore[reportPrivateUsage]
            await _settled(server)
            server._on_signal(signal.SIGINT, None)  # pyright: ignore[reportPrivateUsage]

            for _ in range(100):  # bounded: the cancel lands within a few loop turns
                if runtime.cancelled:
                    break

                await asyncio.sleep(0.01)

        # The drain observed its own cancellation.
        assert runtime.cancelled is True
        assert (server.should_exit, server.force_exit) == (True, True)


    # ....................... #

    def test_off_the_main_thread_the_stop_event_drains_and_stops_the_server(self) -> None:
        """No signal handler can be installed there; the event is the way in."""

        events: list[str] = []
        port = _free_port()

        async def phase(_allowed: timedelta) -> None:
            events.append("phase")

        async def startup(ctx: ExecutionContext) -> None:
            ctx.drain_phases.register("phase", phase)

        async def teardown(_ctx: ExecutionContext) -> None:
            events.append("teardown")

        runtime = build_runtime(
            lifecycle_steps=LifecycleStep(id="step", startup=startup, shutdown=teardown),
            drain_timeout=timedelta(0),
        )
        app = FastAPI(lifespan=runtime_lifespan(runtime))

        async def worker() -> None:
            stop = asyncio.Event()
            config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning")
            serving = asyncio.create_task(
                serve_uvicorn(config, runtime, drain_budget=timedelta(seconds=1), stop=stop)
            )

            for _ in range(200):  # bounded wait for startup to finish
                if runtime.ready:
                    break

                await asyncio.sleep(0.02)

            assert runtime.ready
            stop.set()

            await asyncio.wait_for(serving, timeout=10.0)
            events.append("served")

        failures: list[BaseException] = []

        def run() -> None:
            try:
                asyncio.run(worker())

            except BaseException as error:  # surfaced to the test thread below
                failures.append(error)

        thread = threading.Thread(target=run)
        thread.start()
        thread.join(timeout=20.0)

        assert not thread.is_alive()
        assert failures == []
        assert events == ["phase", "teardown", "served"]
