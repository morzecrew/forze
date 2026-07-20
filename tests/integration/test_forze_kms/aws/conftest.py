"""Pytest configuration for forze_kms.aws integration tests (floci KMS).

The suite runs against floci's KMS — an independent reimplementation of the
KMS wire protocol with full envelope + rotation support (see
``tests/support/floci.py`` for why it replaced LocalStack, what was verified
about its fidelity, and the one accepted gap: no server-side ``KeyId``
enforcement on ``Decrypt``, a guard the keyring already applies client-side).
"""

from collections.abc import AsyncGenerator

import pytest
import pytest_asyncio
from docker import from_env
from docker.errors import DockerException

pytest.importorskip("aioboto3")
pytest.importorskip("testcontainers")

from forze_kms.aws import AwsKmsClient
from tests.support.floci import FlociContainer

_REGION = "us-east-1"


def _ensure_docker_available() -> None:
    client = None

    try:
        client = from_env()
        client.ping()

    except DockerException as exc:
        pytest.skip(f"Docker is required for AWS KMS integration tests: {exc}")

    finally:
        if client is not None:
            client.close()


@pytest.fixture(scope="session")
def floci_container() -> FlociContainer:
    """Start a floci container serving KMS."""

    _ensure_docker_available()

    with FlociContainer() as floci:
        yield floci


@pytest_asyncio.fixture(scope="function")
async def kms_client(
    floci_container: FlociContainer,
) -> AsyncGenerator[AwsKmsClient]:
    """Provide an initialized AWS KMS client connected to the emulator."""

    endpoint = floci_container.get_url()

    client = AwsKmsClient()
    await client.initialize(
        endpoint=endpoint,
        region_name=_REGION,
        access_key_id="test",
        secret_access_key="test",
    )

    yield client

    await client.close()


@pytest_asyncio.fixture(scope="function")
async def cmk_id(kms_client: AwsKmsClient) -> str:
    """Create a symmetric CMK and return its key id (isolated per test)."""

    async with kms_client.client() as c:
        resp = await c.create_key(Description="forze-awskms-it")

    return resp["KeyMetadata"]["KeyId"]
