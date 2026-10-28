"""Pytest configuration for forze_duckdb integration tests (object storage via RustFS)."""

import shutil
import time
import urllib.error
import urllib.request

import pytest

pytest.importorskip("duckdb")
pytest.importorskip("pyarrow")
pytest.importorskip("testcontainers")


from tests.support.rustfs import RustfsContainer


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Skip ``lake_slow`` tests unless the marker expression names them.

    ``just test -m integration`` replaces the recipe's ``not lake_slow``, since pytest keeps
    the last ``-m``, so the exclusion alone would run the 17-minute expiry battery.
    """

    if "lake_slow" in (config.getoption("markexpr") or ""):
        return

    skip = pytest.mark.skip(reason="lake_slow runs only when selected: `just lake-slow`")

    for item in items:
        if item.get_closest_marker("lake_slow") is not None:
            item.add_marker(skip)

S3_ACCESS_KEY = "minioadmin"
S3_SECRET_KEY = "minioadmin"


@pytest.fixture(scope="session")
def s3_container():
    """Start a RustFS container and yield ``(container, host:port endpoint)``."""

    if shutil.which("docker") is None:
        pytest.skip("Docker is required for DuckDB object-storage integration tests")

    with RustfsContainer(
        port=9000,
        access_key=S3_ACCESS_KEY,
        secret_key=S3_SECRET_KEY,
    ) as container:
        host = container.get_container_host_ip()
        port = container.get_exposed_port(9000)
        endpoint = f"{host}:{port}"

        health_url = f"http://{endpoint}/minio/health/live"
        deadline = time.time() + 60

        while time.time() < deadline:
            try:
                with urllib.request.urlopen(health_url, timeout=2) as resp:
                    if resp.status == 200:
                        break
            except (urllib.error.URLError, TimeoutError, OSError):
                time.sleep(0.5)
        else:
            raise RuntimeError("RustFS container did not become healthy in time")

        yield container, endpoint
