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
