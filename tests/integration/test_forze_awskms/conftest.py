"""Pytest configuration for forze_awskms integration tests (LocalStack KMS)."""

from typing import AsyncGenerator

import pytest
import pytest_asyncio
from docker import from_env
from docker.errors import DockerException
from testcontainers.localstack import LocalStackContainer

pytest.importorskip("aioboto3")
pytest.importorskip("testcontainers.localstack")

from forze_awskms import AwsKmsClient

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
def localstack_container() -> LocalStackContainer:
    """Start a LocalStack container with the KMS service enabled."""

    _ensure_docker_available()

    with LocalStackContainer(image="localstack/localstack:3.8.1").with_services(
        "kms"
    ) as localstack:
        yield localstack


@pytest_asyncio.fixture(scope="function")
async def kms_client(
    localstack_container: LocalStackContainer,
) -> AsyncGenerator[AwsKmsClient]:
    """Provide an initialized AWS KMS client connected to LocalStack."""

    endpoint = localstack_container.get_url()

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
