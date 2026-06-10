"""Pytest configuration for forze_duckdb integration tests (object storage via MinIO)."""

import shutil
import time
import urllib.error
import urllib.request

import pytest

pytest.importorskip("duckdb")
pytest.importorskip("pyarrow")
pytest.importorskip("testcontainers")

from testcontainers.minio import MinioContainer

MINIO_ROOT_USER = "minioadmin"
MINIO_ROOT_PASSWORD = "minioadmin"  # noqa: S105


@pytest.fixture(scope="session")
def minio_container():
    """Start a MinIO container and yield ``(container, host:port endpoint)``."""

    if shutil.which("docker") is None:
        pytest.skip("Docker is required for DuckDB object-storage integration tests")

    with MinioContainer(
        image="minio/minio:RELEASE.2025-09-07T16-13-09Z",
        port=9000,
        access_key=MINIO_ROOT_USER,
        secret_key=MINIO_ROOT_PASSWORD,
    ) as container:
        host = container.get_container_host_ip()
        port = container.get_exposed_port(9000)
        endpoint = f"{host}:{port}"

        health_url = f"http://{endpoint}/minio/health/live"
        deadline = time.time() + 60

        while time.time() < deadline:
            try:
                with urllib.request.urlopen(health_url, timeout=2) as resp:  # noqa: S310
                    if resp.status == 200:
                        break
            except (urllib.error.URLError, TimeoutError, OSError):
                time.sleep(0.5)
        else:
            raise RuntimeError("MinIO container did not become healthy in time")

        yield container, endpoint
