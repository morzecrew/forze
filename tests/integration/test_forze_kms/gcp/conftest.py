"""Pytest configuration for forze_kms.gcp integration tests (fake-cloud-kms emulator)."""

from typing import AsyncGenerator
from uuid import uuid4

import pytest
import pytest_asyncio
from docker import from_env
from docker.errors import DockerException
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

pytest.importorskip("google.cloud.kms")
pytest.importorskip("grpc")

from forze_kms.gcp import GcpKmsClient

_PROJECT = "forze-test"
_LOCATION = "global"
_EMULATOR_PORT = 9010
# winor30/fake-cloud-kms: an unofficial gRPC Google Cloud KMS emulator for CI.
_EMULATOR_IMAGE = "winor30/fake-cloud-kms:latest"


def _ensure_docker_available() -> None:
    client = None

    try:
        client = from_env()
        client.ping()

    except DockerException as exc:
        pytest.skip(f"Docker is required for GCP KMS integration tests: {exc}")

    finally:
        if client is not None:
            client.close()


@pytest.fixture(scope="session")
def kms_emulator() -> DockerContainer:
    """Start the fake-cloud-kms gRPC emulator."""

    _ensure_docker_available()

    container = DockerContainer(_EMULATOR_IMAGE).with_exposed_ports(_EMULATOR_PORT)
    container.start()

    try:
        wait_for_logs(container, "Cloud KMS emulator listening", timeout=60)
        yield container

    finally:
        container.stop()


@pytest_asyncio.fixture(scope="function")
async def gcp_kms_client(
    kms_emulator: DockerContainer,
) -> AsyncGenerator[GcpKmsClient]:
    """Provide an initialized GCP KMS client connected to the emulator."""

    host = kms_emulator.get_container_host_ip()
    port = kms_emulator.get_exposed_port(_EMULATOR_PORT)

    client = GcpKmsClient()
    await client.initialize(endpoint=f"{host}:{port}")

    yield client

    await client.close()


async def _create_crypto_key(client: GcpKmsClient, ring_id: str, key_id: str) -> str:
    """Provision a symmetric CryptoKey and return its resource name."""

    from google.cloud.kms_v1.types import CryptoKey

    location = f"projects/{_PROJECT}/locations/{_LOCATION}"
    ring = f"{location}/keyRings/{ring_id}"

    async with client.client() as c:
        await c.create_key_ring(parent=location, key_ring_id=ring_id, key_ring={})
        await c.create_crypto_key(
            parent=ring,
            crypto_key_id=key_id,
            crypto_key=CryptoKey(
                purpose=CryptoKey.CryptoKeyPurpose.ENCRYPT_DECRYPT
            ),
        )

    return f"{ring}/cryptoKeys/{key_id}"


@pytest_asyncio.fixture(scope="function")
async def cmk_name(gcp_kms_client: GcpKmsClient) -> str:
    """Create a symmetric CryptoKey and return its resource name (isolated per test)."""

    return await _create_crypto_key(
        gcp_kms_client, f"ring-{uuid4().hex[:8]}", f"cmk-{uuid4().hex[:8]}"
    )


@pytest.fixture(scope="function")
def create_crypto_key():
    """Expose the CryptoKey provisioning helper to tests (for multi-key scenarios)."""

    return _create_crypto_key
