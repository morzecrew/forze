"""Fixtures for example integration tests (ephemeral Postgres + Redis)."""

import pytest
import pytest_asyncio
from testcontainers.postgres import PostgresContainer
from testcontainers.redis import RedisContainer

pytest.importorskip("psycopg")
pytest.importorskip("redis")

from forze_postgres import PostgresClient, PostgresConfig
from forze_redis import RedisClient, RedisConfig
from tests.support.docker import ensure_docker_available


@pytest.fixture(scope="session")
def postgres_container():
    ensure_docker_available()
    with PostgresContainer(image="postgres:18", driver="psycopg") as postgres:
        yield postgres


@pytest.fixture(scope="session")
def redis_container():
    ensure_docker_available()
    with RedisContainer(image="redis:7-alpine") as redis:
        yield redis


@pytest_asyncio.fixture(scope="function")
async def pg_client(postgres_container):
    url = postgres_container.get_connection_url()
    if url.startswith("postgresql+psycopg://"):
        url = url.replace("postgresql+psycopg://", "postgresql://")

    client = PostgresClient()
    await client.initialize(dsn=url, config=PostgresConfig(min_size=1, max_size=5))
    yield client
    await client.close()


@pytest_asyncio.fixture(scope="function")
async def redis_client(redis_container):
    host = redis_container.get_container_host_ip()
    port = redis_container.get_exposed_port(6379)

    client = RedisClient()
    await client.initialize(dsn=f"redis://{host}:{port}/0", config=RedisConfig(max_size=5))
    yield client
    await client.close()
