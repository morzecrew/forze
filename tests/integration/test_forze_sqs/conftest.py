"""Pytest configuration for forze_sqs integration tests.

The suite runs against floci's SQS — an independent reimplementation of the
SQS wire protocol (see ``tests/support/floci.py`` for why it replaced
LocalStack and what was verified about its fidelity).
"""

from typing import AsyncGenerator
from uuid import uuid4

import pytest
import pytest_asyncio
from docker import from_env
from docker.errors import DockerException
from pydantic import BaseModel

pytest.importorskip("aioboto3")
pytest.importorskip("testcontainers")

from forze_sqs.adapters import SQSQueueAdapter, SQSQueueCodec
from forze_sqs.kernel.client import SQSClient
from forze.base.serialization import PydanticModelCodec
from tests.support.floci import FlociContainer


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
def floci_container() -> FlociContainer:
    """Start a floci container serving SQS."""
    _ensure_docker_available()

    with FlociContainer() as floci:
        yield floci


@pytest_asyncio.fixture(scope="function")
async def sqs_client(
    floci_container: FlociContainer,
) -> AsyncGenerator[SQSClient]:
    """Provide an initialized SQS client connected to the emulator."""
    endpoint = floci_container.get_url()

    client = SQSClient()
    await client.initialize(
        endpoint=endpoint,
        region_name="us-east-1",
        access_key_id="test",
        secret_access_key="test",
    )

    yield client

    await client.close()


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
        codec=SQSQueueCodec(payload_codec=PydanticModelCodec(_QueuePayload)),
        namespace=namespace,
    )


@pytest.fixture(scope="function")
def queue_payload_cls() -> type[_QueuePayload]:
    """Provide the queue payload model for constructing test messages."""

    return _QueuePayload
