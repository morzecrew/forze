"""Pytest configuration for forze_gcs integration tests."""

import os
import shutil
from uuid import uuid4

import pytest
import pytest_asyncio

pytest.importorskip("gcloud.aio.storage")
pytest.importorskip("testcontainers")

from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

from forze_gcs.kernel.client.client import GCSClient

FAKE_GCS_IMAGE = "fsouza/fake-gcs-server:latest"
FAKE_GCS_PORT = 4443
TEST_PROJECT_ID = "forze-gcs-test"


def _ensure_docker() -> None:
    if shutil.which("docker") is None:
        pytest.skip("Docker is required for GCS integration tests")


@pytest.fixture(scope="session")
def fake_gcs_container():
    """Starts fake-gcs-server for GCS integration tests."""
    _ensure_docker()

    container = (
        DockerContainer(image=FAKE_GCS_IMAGE)
        .with_command(
            [
                "-scheme",
                "http",
                "-port",
                str(FAKE_GCS_PORT),
                "-public-host",
                "0.0.0.0",
            ]
        )
        .with_exposed_ports(FAKE_GCS_PORT)
    )
    container.start()

    wait_for_logs(container, "server started at", timeout=60)

    host = container.get_container_host_ip()
    port = container.get_exposed_port(FAKE_GCS_PORT)
    external_url = f"http://{host}:{port}"

    os.environ["STORAGE_EMULATOR_HOST"] = external_url

    yield external_url

    container.stop()


@pytest_asyncio.fixture(scope="function")
async def gcs_client(fake_gcs_container: str) -> GCSClient:
    """Provides an initialized GCS client connected to fake-gcs-server."""
    client = GCSClient()
    _ = fake_gcs_container
    await client.initialize(TEST_PROJECT_ID)

    yield client

    await client.close()


@pytest_asyncio.fixture(scope="function")
async def gcs_bucket(gcs_client: GCSClient) -> str:
    """Creates and returns an isolated bucket for a test."""
    bucket = f"forze-gcs-{uuid4().hex[:16]}"

    async with gcs_client.client():
        await gcs_client.create_bucket(bucket)

    return bucket
