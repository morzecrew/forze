"""Pytest configuration for forze_sqs integration tests."""

from uuid import uuid4

import pytest
import pytest_asyncio
from docker import from_env
from docker.errors import DockerException
from pydantic import BaseModel
from testcontainers.localstack import LocalStackContainer

pytest.importorskip("aioboto3")
pytest.importorskip("testcontainers.localstack")

from forze_sqs.adapters import SQSQueueAdapter, SQSQueueCodec
from forze_sqs.kernel.platform import SQSClient


def _ensure_docker_available() -> None:
    client = None

    try:
        client = from_env()
        client.ping()
    except DockerException as exc:
        pytest.skip(f"Docker is required for SQS integration tests: {exc}")
    finally:
        if client is not None:
            client.close()


@pytest.fixture(scope="session")
def localstack_container() -> LocalStackContainer:
    """Start a LocalStack container with SQS enabled."""
    _ensure_docker_available()

    with LocalStackContainer(image="localstack/localstack:3.8.1").with_services(
        "sqs"
    ) as localstack:
        yield localstack


@pytest_asyncio.fixture(scope="function")
async def sqs_client(localstack_container: LocalStackContainer) -> SQSClient:
    """Provide an initialized SQS client connected to LocalStack."""
    endpoint = localstack_container.get_url()

    client = SQSClient()
    await client.initialize(
        endpoint=endpoint,
        region_name="us-east-1",
        access_key_id="test",
        secret_access_key="test",
    )

    yield client

    client.close()


@pytest_asyncio.fixture(scope="function")
async def sqs_queue_url(sqs_client: SQSClient) -> str:
    """Create and return an isolated queue URL for a test."""
    queue = f"forze-sqs-{uuid4().hex[:12]}"

    async with sqs_client.client():
        return await sqs_client.create_queue(queue)


class _QueuePayload(BaseModel):
    value: str


@pytest_asyncio.fixture(scope="function")
async def sqs_queue(sqs_client: SQSClient) -> SQSQueueAdapter[_QueuePayload]:
    """Provide a queue adapter with a unique namespace per test."""
    namespace = f"itsqs-{uuid4().hex[:10]}"

    return SQSQueueAdapter(
        client=sqs_client,
        codec=SQSQueueCodec(model=_QueuePayload),
        namespace=namespace,
    )


@pytest.fixture(scope="function")
def queue_payload_cls() -> type[_QueuePayload]:
    """Provide the queue payload model for constructing test messages."""
    return _QueuePayload
