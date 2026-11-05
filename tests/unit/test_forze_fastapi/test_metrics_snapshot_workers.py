"""``uvicorn --workers N`` writing metric snapshots to one directory: every scrape, whichever
worker takes it, covers every live worker; a killed worker ages out; two processes sharing
one instance id are withheld (RFC 0083).

Built to hold on a loaded box. A worker the scheduler sets aside for a while is still a live
worker, so the ttl leaves it twenty intervals of slack before its snapshot lapses. Every wait
polls against a deadline rather than sleeping, and every claim about the workers is made on
the scrape that showed them.
"""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager, suppress
from functools import partial
from pathlib import Path

import httpx
import pytest
from prometheus_client.parser import text_string_to_metric_families

from tests.support.ports import free_port

# ----------------------- #

INTERVAL = 0.25
TTL = 5.0  # twenty intervals: a stalled but live worker keeps its place in the union
STARTUP = 120.0  # three spawned workers importing the framework on a busy machine
_APP_DIR = str(Path(__file__).parent)
_BIND_ATTEMPTS = 3

pytestmark = pytest.mark.skipif(sys.platform == "win32", reason="POSIX pre-fork workers")


def _wait_for[T](condition: str, check: Callable[[], T | None], *, within: float) -> T:
    """Poll *check* until it returns something other than ``None`` or ``False``."""

    deadline = time.monotonic() + within

    while time.monotonic() < deadline:
        result = check()

        if result is not None and result is not False:
            return result

        time.sleep(0.05)

    raise AssertionError(f"timed out after {within}s waiting for {condition}")


def _scrape(port: int) -> httpx.Response | None:
    try:
        return httpx.get(f"http://127.0.0.1:{port}/metrics", timeout=10.0)

    except httpx.HTTPError:
        return None


def _pids(response: httpx.Response | None) -> dict[str, int]:
    """``service_instance_id`` to the pid that worker reports."""

    if response is None or response.status_code != 200:
        return {}

    return {
        sample.labels["service_instance_id"]: int(sample.value)
        for family in text_string_to_metric_families(response.text)
        if family.name == "forze_test_pid"
        for sample in family.samples
    }


def _hits(response: httpx.Response) -> dict[str, float]:
    return {
        sample.labels["service_instance_id"]: sample.value
        for family in text_string_to_metric_families(response.text)
        if family.name == "forze_test_hits"
        for sample in family.samples
        if sample.name == "forze_test_hits_total"
    }


def _group(pid: int) -> int | None:
    try:
        return os.getpgid(pid)

    except ProcessLookupError:
        return None


def _started(process: subprocess.Popen[bytes], port: int) -> str | bool:
    """``exited`` once uvicorn has exited, as it does when it cannot bind its port.

    Otherwise, whether *port* answers from a worker in this server's process group, so another
    server that took the port is never mistaken for this one.
    """

    if process.poll() is not None:
        return "exited"

    try:
        pid = httpx.get(f"http://127.0.0.1:{port}/pid", timeout=10.0).json()["pid"]

    except (httpx.HTTPError, ValueError, KeyError, TypeError):
        return False

    return _group(pid) == process.pid


@contextmanager
def _serve(tmp_path: Path, *, workers: int, **env: str) -> Iterator[int]:
    stderr_path = tmp_path / "stderr"

    with stderr_path.open("wb") as stderr:
        for attempt in range(_BIND_ATTEMPTS):
            # Another process can take a free port before uvicorn binds it; uvicorn then
            # exits, and the server starts again on a fresh port.
            port = free_port()
            process = subprocess.Popen(
                [
                    sys.executable,
                    "-m",
                    "uvicorn",
                    "_metrics_workers_app:create_app",
                    "--factory",
                    "--app-dir",
                    _APP_DIR,
                    "--workers",
                    str(workers),
                    "--host",
                    "127.0.0.1",
                    "--port",
                    str(port),
                    "--log-level",
                    "warning",
                    # A worker slow to answer the supervisor's ping on a busy box is not
                    # hung; replacing it would change the instances under the test.
                    "--timeout-worker-healthcheck",
                    "60",
                ],
                env={
                    **os.environ,
                    "FORZE_TEST_SNAPSHOTS": str(tmp_path / f"snaps-{attempt}"),
                    "FORZE_TEST_INTERVAL": str(INTERVAL),
                    "FORZE_TEST_TTL": str(TTL),
                    **env,
                },
                stdout=subprocess.DEVNULL,
                stderr=stderr,
                start_new_session=True,
            )

            try:
                started = _wait_for(
                    "the server to answer or exit",
                    partial(_started, process, port),
                    within=STARTUP,
                )

                if started == "exited" and attempt + 1 < _BIND_ATTEMPTS:
                    continue

                assert started is True, "uvicorn exited before it served"

                yield port
                return

            except BaseException:
                sys.stderr.write(stderr_path.read_text())
                raise

            finally:
                # The whole session: killing the supervisor alone would leave the workers.
                with suppress(ProcessLookupError):
                    os.killpg(process.pid, signal.SIGKILL)

                process.wait(timeout=30)


def _snaps(tmp_path: Path) -> Path:
    """The snapshot directory of the attempt that bound its port."""

    return max(tmp_path.glob("snaps-*"), key=lambda path: path.name)


def _all_workers(port: int, count: int) -> dict[str, int] | None:
    pids = _pids(_scrape(port))

    return pids if len(pids) == count else None


# ----------------------- #


def test_every_scrape_covers_every_worker_and_a_killed_one_ages_out(tmp_path: Path) -> None:
    with _serve(tmp_path, workers=3) as port:
        _wait_for("three workers in one scrape", lambda: _all_workers(port, 3), within=STARTUP)

        for _ in range(60):
            httpx.get(f"http://127.0.0.1:{port}/hit", timeout=10.0)

        seen: dict[str, float] = {}

        for _ in range(20):
            response = _scrape(port)
            assert response is not None and response.status_code == 200

            # Whichever worker took the scrape, it answered for all three.
            assert len(_pids(response)) == 3

            # Each worker's counter is its own series and never goes backwards: no
            # interleaving, so no phantom reset for rate() to see.
            for instance, value in _hits(response).items():
                assert value >= seen.get(instance, 0.0), instance
                seen[instance] = value

            ages = {
                sample.labels["service_instance_id"]: sample.value
                for family in text_string_to_metric_families(response.text)
                if family.name == "forze_metrics_snapshot_age_seconds"
                for sample in family.samples
            }
            assert set(ages) == set(_pids(response))
            assert all(0.0 <= age <= TTL for age in ages.values())

            # Pacing, not waiting: the scrapes span several of each worker's writes.
            time.sleep(INTERVAL / 2)

        def every_hit_counted() -> bool:
            response = _scrape(port)

            if response is None or response.status_code != 200:
                return False

            return sum(_hits(response).values()) == 60

        # Each worker's last snapshot can trail its last hit by an interval, or by however
        # long the scheduler keeps it from writing.
        _wait_for("every hit in one scrape", every_hit_counted, within=STARTUP)

        before = _wait_for("three workers again", lambda: _all_workers(port, 3), within=STARTUP)
        victim, victim_pid = next(iter(before.items()))
        os.kill(victim_pid, signal.SIGKILL)

        def recovered() -> dict[str, int] | None:
            now = _all_workers(port, 3)

            return now if now is not None and victim not in now else None

        # The killed worker's snapshot stops being served once older than the ttl; uvicorn
        # replaces the worker, which comes back as a new instance, not as the old one.
        after = _wait_for("the killed worker to age out and be replaced", recovered, within=STARTUP)

        assert len(set(after) & set(before)) == 2


def test_two_workers_sharing_an_instance_id_are_withheld(tmp_path: Path) -> None:
    with _serve(tmp_path, workers=2, FORZE_TEST_INSTANCE_ID="shared-by-preload") as port:
        snaps = _snaps(tmp_path)
        _wait_for(
            "both workers' snapshots",
            lambda: snaps.exists() and len(list(snaps.glob("*.prom"))) == 2,
            within=STARTUP,
        )

        headers = {path.read_text().partition("\n")[0] for path in snaps.glob("*.prom")}
        assert all(header.endswith("instance=shared-by-preload") for header in headers)

        for _ in range(10):
            response = _scrape(port)
            assert response is not None
            # Serving either would interleave two processes into one series.
            assert response.status_code == 503


def test_without_snapshots_each_scrape_sees_only_the_worker_that_took_it(
    tmp_path: Path,
) -> None:
    """The bug the snapshot mode fixes, and proof the test above can see it."""

    with _serve(tmp_path, workers=2, FORZE_TEST_PLAIN="1") as port:
        _wait_for("the route", lambda: len(_pids(_scrape(port))) == 1, within=STARTUP)

        assert all(len(_pids(_scrape(port))) == 1 for _ in range(10))
