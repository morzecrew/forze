"""``uvicorn --workers N`` writing metric snapshots to one directory: every scrape, whichever
worker takes it, covers every live worker; a killed worker ages out; two processes sharing
one instance id are withheld (RFC 0083)."""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager, suppress
from pathlib import Path

import httpx
import pytest
from prometheus_client.parser import text_string_to_metric_families

from tests.support.ports import free_port

# ----------------------- #

TTL = 0.6  # the app's
_APP_DIR = str(Path(__file__).parent)

pytestmark = pytest.mark.skipif(sys.platform == "win32", reason="POSIX pre-fork workers")


def _wait_for(condition: str, check: Callable[[], bool], *, within: float) -> None:
    deadline = time.monotonic() + within

    while time.monotonic() < deadline:
        if check():
            return

        time.sleep(0.05)

    raise AssertionError(f"timed out after {within}s waiting for {condition}")


def _scrape(port: int) -> httpx.Response | None:
    try:
        return httpx.get(f"http://127.0.0.1:{port}/metrics", timeout=5.0)

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


@contextmanager
def _serve(tmp_path: Path, *, workers: int, **env: str) -> Iterator[int]:
    port = free_port()
    stderr_path = tmp_path / "stderr"

    with stderr_path.open("wb") as stderr:
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
            ],
            env={**os.environ, "FORZE_TEST_SNAPSHOTS": str(tmp_path / "snaps"), **env},
            stdout=subprocess.DEVNULL,
            stderr=stderr,
            start_new_session=True,
        )

        try:
            yield port

        except BaseException:
            sys.stderr.write(stderr_path.read_text())
            raise

        finally:
            # The whole session: killing the supervisor alone would leave the workers on the port.
            with suppress(ProcessLookupError):
                os.killpg(process.pid, signal.SIGKILL)

            process.wait(timeout=10)


# ----------------------- #


def test_every_scrape_covers_every_worker_and_a_killed_one_ages_out(tmp_path: Path) -> None:
    with _serve(tmp_path, workers=3) as port:
        _wait_for("three workers in one scrape", lambda: len(_pids(_scrape(port))) == 3, within=60)

        for _ in range(60):
            httpx.get(f"http://127.0.0.1:{port}/hit", timeout=5.0)

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

            time.sleep(0.05)

        assert sum(seen.values()) == 60

        before = _pids(_scrape(port))
        victim, victim_pid = next(iter(before.items()))
        os.kill(victim_pid, signal.SIGKILL)

        def recovered() -> bool:
            now = _pids(_scrape(port))
            return victim not in now and len(now) == 3

        # The killed worker's snapshot stops being served once older than the ttl; uvicorn
        # replaces the worker, which comes back as a new instance, not as the old one.
        _wait_for("the killed worker to age out and be replaced", recovered, within=60)

        assert len(set(_pids(_scrape(port))) & set(before)) == 2


def test_two_workers_sharing_an_instance_id_are_withheld(tmp_path: Path) -> None:
    snaps = tmp_path / "snaps"

    with _serve(tmp_path, workers=2, FORZE_TEST_INSTANCE_ID="shared-by-preload") as port:
        _wait_for(
            "both workers' snapshots",
            lambda: snaps.exists() and len(list(snaps.glob("*.prom"))) == 2,
            within=60,
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
        _wait_for("the route", lambda: len(_pids(_scrape(port))) == 1, within=60)

        assert all(len(_pids(_scrape(port))) == 1 for _ in range(10))
