"""``serve_uvicorn`` routes SIGTERM into announce → drain phases → exit, in a real process."""

from __future__ import annotations

import signal
import socket
import subprocess
import sys
import time
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from pathlib import Path

import httpx
import pytest

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
        assert "phase-cancelled" in _events(record)
        assert "phase-done" not in _events(record)
        assert elapsed < 3.0

    # ....................... #

    def test_a_drain_that_raises_still_lets_the_server_exit(self, tmp_path: Path) -> None:
        with _serving(tmp_path, "raise") as (process, _port, record):
            process.send_signal(signal.SIGTERM)
            code = process.wait(timeout=10.0)

        assert code == 0
        assert _events(record)[0] == "drain-raised"
        assert _events(record)[-2:] == ["teardown", "served"]
