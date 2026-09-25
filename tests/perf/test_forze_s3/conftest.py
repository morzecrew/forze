"""Pytest configuration for forze_s3 performance tests."""

import shutil
import time
import urllib.error
import urllib.request
from uuid import uuid4

import pytest
import pytest_asyncio

pytest.importorskip("aioboto3")
pytest.importorskip("testcontainers")


from forze_s3.kernel.client import S3Client, S3Config
from tests.support.rustfs import RustfsContainer

S3_ACCESS_KEY = "minioadmin"
S3_SECRET_KEY = "minioadmin"


@pytest.fixture(scope="session")
def s3_container():
    """Start a RustFS container for S3 performance tests."""
    if shutil.which("docker") is None:
        pytest.skip("Docker is required for S3 performance tests")

    with RustfsContainer(
        port=9000,
        access_key=S3_ACCESS_KEY,
        secret_key=S3_SECRET_KEY,
    ) as container:
        endpoint = f"http://{container.get_container_host_ip()}:{container.get_exposed_port(9000)}"

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
            raise RuntimeError("RustFS container did not become healthy in time")

        yield container, endpoint


@pytest_asyncio.fixture(scope="function")
async def s3_client(s3_container):
    """Provide an initialized S3 client connected to RustFS."""
    _container, endpoint = s3_container

    client = S3Client()
    config = S3Config(s3={"addressing_style": "path"})
    await client.initialize(
        endpoint=endpoint,
        access_key_id=S3_ACCESS_KEY,
        secret_access_key=S3_SECRET_KEY,
        config=config,
    )

    return client


@pytest_asyncio.fixture(scope="function")
async def s3_bucket(s3_client: S3Client) -> str:
    """Create and return an isolated bucket for a benchmark."""
    bucket = f"forze-s3-perf-{uuid4().hex[:16]}"

    async with s3_client.client():
        await s3_client.create_bucket(bucket)

    return bucket
