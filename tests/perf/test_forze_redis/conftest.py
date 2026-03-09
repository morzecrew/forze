"""Pytest configuration for forze_redis performance tests."""

import pytest
import pytest_asyncio
from docker import from_env
from docker.errors import DockerException
from testcontainers.redis import RedisContainer

pytest.importorskip("redis")

from forze_redis.kernel.platform.client import RedisClient, RedisConfig


def _ensure_docker_available() -> None:
    client = None

    try:
        client = from_env()
        client.ping()
    except DockerException as exc:
        pytest.skip(f"Docker is required for Redis performance tests: {exc}")
    finally:
        if client is not None:
            client.close()


@pytest.fixture(scope="session")
def redis_container() -> RedisContainer:
    """Start a Redis container for performance testing."""
    _ensure_docker_available()

    with RedisContainer(image="valkey/valkey:9.0") as redis:
        yield redis


@pytest_asyncio.fixture(scope="function")
async def redis_client(redis_container: RedisContainer) -> RedisClient:
    """Provide an initialized RedisClient connected to test container."""

    host = redis_container.get_container_host_ip()
    port = redis_container.get_exposed_port(6379)
    dsn = f"redis://{host}:{port}/0"

    client = RedisClient()
    await client.initialize(dsn=dsn, config=RedisConfig(max_size=10))

    yield client

    await client.close()
