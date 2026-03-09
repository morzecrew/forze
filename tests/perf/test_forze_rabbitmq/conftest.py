"""Pytest configuration for forze_rabbitmq performance tests."""

from urllib.parse import quote

import pytest
import pytest_asyncio
from docker import from_env
from docker.errors import DockerException
from testcontainers.rabbitmq import RabbitMqContainer

pytest.importorskip("aio_pika")

from forze_rabbitmq.kernel.platform import RabbitMQClient, RabbitMQConfig


def _ensure_docker_available() -> None:
    client = None

    try:
        client = from_env()
        client.ping()
    except DockerException as exc:
        pytest.skip(f"Docker is required for RabbitMQ performance tests: {exc}")
    finally:
        if client is not None:
            client.close()


def _rabbitmq_dsn(container: RabbitMqContainer) -> str:
    host = container.get_container_host_ip()
    port = container.get_exposed_port(container.port)
    vhost = quote(container.vhost, safe="")

    return f"amqp://{container.username}:{container.password}@{host}:{port}/{vhost}"


@pytest.fixture(scope="session")
def rabbitmq_container() -> RabbitMqContainer:
    """Start a RabbitMQ container for performance testing."""
    _ensure_docker_available()

    with RabbitMqContainer(image="rabbitmq:3.13-management") as rabbit:
        yield rabbit


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
