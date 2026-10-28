"""``uvicorn --workers 2`` serving an app factory whose lifespan builds the runtime: SIGTERM
drains every worker's runtime while it still serves, before it stops."""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import threading
import time
from collections import defaultdict
from pathlib import Path

import httpx
import pytest

from tests.support.ports import free_port

# ----------------------- #

ANNOUNCE_DELAY = 1.5
SLOW_REQUEST = 4.0


def _events(record: Path) -> dict[int, list[str]]:
    by_pid: defaultdict[int, list[str]] = defaultdict(list)

    for line in record.read_text().splitlines() if record.exists() else []:
        pid, event = line.split()
        by_pid[int(pid)].append(event)

    return by_pid


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


# ----------------------- #


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX signals")
def test_sigterm_drains_every_worker_before_it_stops(tmp_path: Path) -> None:
    port = free_port()
    record = tmp_path / "events"
    replies: list[httpx.Response] = []

    def started_workers() -> int:
        return sum("started" in events for events in _events(record).values())

    def slow_request() -> None:
        replies.append(httpx.get(f"http://127.0.0.1:{port}/slow", timeout=SLOW_REQUEST + 10))

    with (tmp_path / "stderr").open("wb") as stderr:
        process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "uvicorn",
                "_serve_workers_app:create_app",
                "--factory",
                "--app-dir",
                str(Path(__file__).parent),
                "--workers",
                "2",
                "--host",
                "127.0.0.1",
                "--port",
                str(port),
                "--log-level",
                "warning",
            ],
            env={**os.environ, "FORZE_TEST_RECORD": str(record)},
            stdout=subprocess.DEVNULL,
            stderr=stderr,
        )

        try:
            _wait_for("both workers", lambda: started_workers() == 2, within=30.0)
            _wait_for("readiness", lambda: _readiness(port) == 200, within=10.0)

            in_flight = threading.Thread(target=slow_request)
            in_flight.start()
            _wait_for(
                "the slow request",
                lambda: any("slow-started" in e for e in _events(record).values()),
                within=5.0,
            )

            signalled = time.monotonic()
            process.send_signal(signal.SIGTERM)

            # Still serving, but no longer ready: the balancer's cue to stop routing.
            _wait_for("readiness to fail", lambda: _readiness(port) == 503, within=ANNOUNCE_DELAY)

            in_flight.join(timeout=SLOW_REQUEST + 10)

            # Past the drain the workers no longer accept: refused, or closed unanswered.
            with pytest.raises(httpx.TransportError):
                httpx.get(f"http://127.0.0.1:{port}/readyz", timeout=10.0)

            code = process.wait(timeout=10.0)
            elapsed = time.monotonic() - signalled

        except BaseException:
            sys.stderr.write((tmp_path / "stderr").read_text())
            raise

        finally:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=10)

    assert code == 0
    assert [reply.status_code for reply in replies] == [200]

    events = _events(record)
    served_by = replies[0].json()["pid"]

    # Each worker built its own runtime, drained it on the signal, then tore it down.
    assert len(events) == 2

    for pid, seen in events.items():
        tail = [e for e in seen if not e.startswith("slow-")]
        assert tail == ["built", "started", "phase-started", "phase-done", "teardown"], pid

    # The in-flight request outlived the drain and finished before the teardown.
    slow = events[served_by]
    assert slow.index("slow-started") < slow.index("phase-started")
    assert slow.index("phase-done") < slow.index("slow-done") < slow.index("teardown")

    # The supervisor notices the signal within half a second; the request held the exit.
    assert elapsed < 0.5 + SLOW_REQUEST + 5.0
