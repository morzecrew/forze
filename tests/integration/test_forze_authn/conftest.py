"""Pytest configuration for ``forze_identity.authn`` integration tests."""

import pytest
import pytest_asyncio
from docker import from_env
from docker.errors import DockerException
from testcontainers.postgres import PostgresContainer

pytest.importorskip("psycopg")

from forze_postgres.kernel.client.client import PostgresClient, PostgresConfig


def _ensure_docker_available() -> None:
    client = None
    try:
        client = from_env()
        client.ping()
    except DockerException as exc:
        pytest.skip(f"Docker is required: {exc}")
    finally:
        if client is not None:
            client.close()


@pytest.fixture(scope="session")
def postgres_container():
    """Postgres container (same pattern as ``test_forze_postgres``)."""
    _ensure_docker_available()
    with PostgresContainer(image="ghcr.io/morzecrew/postgres:18", driver="psycopg") as postgres:
        yield postgres


@pytest_asyncio.fixture(scope="function")
async def pg_client(postgres_container):
    """Initialized :class:`PostgresClient` for integration tests."""

    url = postgres_container.get_connection_url()
    if url.startswith("postgresql+psycopg://"):
        url = url.replace("postgresql+psycopg://", "postgresql://")

    client = PostgresClient()
    await client.initialize(dsn=url, config=PostgresConfig(min_size=1, max_size=5))

    yield client

    await client.close()


@pytest.fixture(scope="module")
def redis_container():
    """A Redis-compatible server for the session fast path's states."""

    _ensure_docker_available()

    from testcontainers.redis import RedisContainer

    with RedisContainer(image="valkey/valkey:9.0") as redis:
        yield redis


@pytest_asyncio.fixture
async def redis_client(redis_container):
    """Initialized ``RedisClient`` on :func:`redis_container`."""

    pytest.importorskip("redis")

    from forze_redis import RedisClient, RedisConfig

    host = redis_container.get_container_host_ip()
    port = redis_container.get_exposed_port(6379)
    client = RedisClient()
    await client.initialize(dsn=f"redis://{host}:{port}/0", config=RedisConfig(max_size=5))

    yield client

    await client.close()
