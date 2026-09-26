"""Pytest configuration for forze_s3 integration tests.

The suite runs against an implementation matrix — RustFS and floci-S3, two
independent implementations of the S3 wire protocol — so nothing here quietly
specializes to one server's behavior (multipart/ETag composition, ranged
reads, listing pagination, conditional writes). A divergence between backends
is a finding to fix in the adapter or declare, never to special-case per
backend in ``src/``. See ``tests/support/floci.py`` for the floci rationale.

The SSE suite (``test_s3_sse.py``) stays RustFS-only by design: its fixture
configures RustFS's master key and static KMS, which is server-specific setup.
"""

import shutil
import time
import urllib.error
import urllib.request
from collections.abc import Iterator
from typing import NamedTuple
from uuid import uuid4

import pytest
import pytest_asyncio

pytest.importorskip("aioboto3")
pytest.importorskip("testcontainers")


from forze_s3.kernel.client import S3Client, S3Config
from tests.support.floci import FlociContainer
from tests.support.rustfs import RustfsContainer

S3_ACCESS_KEY = "minioadmin"
S3_SECRET_KEY = "minioadmin"


class S3Backend(NamedTuple):
    """Connection facts of one S3 implementation under test."""

    name: str
    endpoint: str
    access_key: str
    secret_key: str


def _wait_http_ok(url: str, *, timeout_s: float = 60) -> None:
    deadline = time.time() + timeout_s

    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=2) as resp:
                if resp.status == 200:
                    return
        except (urllib.error.URLError, TimeoutError, OSError):
            time.sleep(0.5)

    raise RuntimeError(f"S3 backend did not become healthy in time: {url}")


@pytest.fixture(scope="session", params=["rustfs", "floci"])
def s3_backend(request: pytest.FixtureRequest) -> Iterator[S3Backend]:
    """One S3 implementation per param; every test runs against both."""

    if shutil.which("docker") is None:
        pytest.skip("Docker is required for S3 integration tests")

    if request.param == "rustfs":
        with RustfsContainer(
            port=9000,
            access_key=S3_ACCESS_KEY,
            secret_key=S3_SECRET_KEY,
        ) as container:
            endpoint = (
                f"http://{container.get_container_host_ip()}:{container.get_exposed_port(9000)}"
            )
            _wait_http_ok(f"{endpoint}/minio/health/live")

            yield S3Backend("rustfs", endpoint, S3_ACCESS_KEY, S3_SECRET_KEY)

    else:
        with FlociContainer() as floci:
            yield S3Backend("floci", floci.get_url(), "test", "test")


@pytest_asyncio.fixture(scope="function")
async def s3_client(s3_backend: S3Backend):
    """Provides an initialized S3 client connected to the backend under test."""

    client = S3Client()
    config = S3Config(s3={"addressing_style": "path"})
    await client.initialize(
        endpoint=s3_backend.endpoint,
        access_key_id=s3_backend.access_key,
        secret_access_key=s3_backend.secret_key,
        config=config,
    )

    yield client

    await client.close()


@pytest_asyncio.fixture(scope="function")
async def s3_bucket(s3_client: S3Client) -> str:
    """Creates and returns an isolated bucket for a test."""
    bucket = f"forze-s3-{uuid4().hex[:16]}"

    async with s3_client.client():
        await s3_client.create_bucket(bucket)

    return bucket
