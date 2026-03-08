"""Pytest configuration for forze_s3 integration tests."""

import shutil
import time
import urllib.error
import urllib.request
from uuid import uuid4

import pytest
import pytest_asyncio

pytest.importorskip("aioboto3")
pytest.importorskip("testcontainers")

from testcontainers.core.container import DockerContainer

from forze_s3.kernel.platform.client import S3Client, S3Config

MINIO_IMAGE = "minio/minio:latest"
MINIO_ROOT_USER = "minioadmin"
MINIO_ROOT_PASSWORD = "minioadmin"


@pytest.fixture(scope="session")
def minio_container():
    """Starts a MinIO container for S3 integration tests."""
    if shutil.which("docker") is None:
        pytest.skip("Docker is required for S3 integration tests")

    with (
        DockerContainer(MINIO_IMAGE)
        .with_env("MINIO_ROOT_USER", MINIO_ROOT_USER)
        .with_env("MINIO_ROOT_PASSWORD", MINIO_ROOT_PASSWORD)
        .with_command("server /data")
        .with_exposed_ports(9000)
    ) as container:
        endpoint = (
            f"http://{container.get_container_host_ip()}:{container.get_exposed_port(9000)}"
        )

        health_url = f"{endpoint}/minio/health/live"
        deadline = time.time() + 60

        while time.time() < deadline:
            try:
                with urllib.request.urlopen(health_url, timeout=2) as resp:
                    if resp.status == 200:
                        break
            except (urllib.error.URLError, TimeoutError, OSError):
                time.sleep(0.5)
        else:
            raise RuntimeError("MinIO container did not become healthy in time")

        yield container, endpoint


@pytest_asyncio.fixture(scope="function")
async def s3_client(minio_container):
    """Provides an initialized S3 client connected to MinIO."""
    _container, endpoint = minio_container

    client = S3Client()
    config: S3Config = {"s3": {"addressing_style": "path"}}
    await client.initialize(
        endpoint=endpoint,
        access_key_id=MINIO_ROOT_USER,
        secret_access_key=MINIO_ROOT_PASSWORD,
        config=config,
    )

    yield client

    client.close()


@pytest_asyncio.fixture(scope="function")
async def s3_bucket(s3_client: S3Client) -> str:
    """Creates and returns an isolated bucket for a test."""
    bucket = f"forze-s3-{uuid4().hex[:16]}"

    async with s3_client.client():
        await s3_client.create_bucket(bucket)

    return bucket
