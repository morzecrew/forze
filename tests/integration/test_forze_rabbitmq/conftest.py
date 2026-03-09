"""Pytest configuration for forze_rabbitmq integration tests."""

from urllib.parse import quote
from uuid import uuid4

import pytest
import pytest_asyncio
from docker import from_env
from docker.errors import DockerException
from pydantic import BaseModel
from testcontainers.rabbitmq import RabbitMqContainer

pytest.importorskip("aio_pika")

from forze_rabbitmq.adapters import RabbitMQQueueAdapter, RabbitMQQueueCodec
from forze_rabbitmq.kernel.platform import RabbitMQClient, RabbitMQConfig


def _ensure_docker_available() -> None:
    client = None

    try:
        client = from_env()
        client.ping()
    except DockerException as exc:
        pytest.skip(f"Docker is required for RabbitMQ integration tests: {exc}")
    finally:
        if client is not None:
            client.close()


@pytest.fixture(scope="session")
def rabbitmq_container() -> RabbitMqContainer:
    """Start a RabbitMQ container for integration tests."""
    _ensure_docker_available()

    with RabbitMqContainer(image="rabbitmq:3.13-management") as rabbit:
        yield rabbit


def _rabbitmq_dsn(container: RabbitMqContainer) -> str:
    host = container.get_container_host_ip()
    port = container.get_exposed_port(container.port)
    vhost = quote(container.vhost, safe="")

    return f"amqp://{container.username}:{container.password}@{host}:{port}/{vhost}"


@pytest_asyncio.fixture(scope="function")
async def rabbitmq_client(rabbitmq_container: RabbitMqContainer) -> RabbitMQClient:
    """Provide an initialized RabbitMQClient connected to test container."""
    dsn = _rabbitmq_dsn(rabbitmq_container)

    client = RabbitMQClient()
    await client.initialize(
        dsn=dsn,
        config=RabbitMQConfig(prefetch_count=20, connect_timeout=10.0),
    )

    yield client

    await client.close()


class _QueuePayload(BaseModel):
    """Minimal payload model for queue integration tests."""

    value: str


@pytest_asyncio.fixture(scope="function")
async def rabbitmq_queue(rabbitmq_client: RabbitMQClient) -> RabbitMQQueueAdapter[_QueuePayload]:
    """Provide a RabbitMQQueueAdapter with a unique namespace per test."""
    namespace = f"it:rabbitmq:{uuid4().hex[:12]}"

    return RabbitMQQueueAdapter(
        client=rabbitmq_client,
        codec=RabbitMQQueueCodec(model=_QueuePayload),
        namespace=namespace,
    )


@pytest.fixture(scope="function")
def queue_payload_cls() -> type[_QueuePayload]:
    """Provide the queue payload model for constructing test messages."""
    return _QueuePayload
