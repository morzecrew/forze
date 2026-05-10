"""Pytest configuration for forze_postgres integration tests."""

from uuid import uuid4

import pytest
import pytest_asyncio
from docker import from_env
from docker.errors import DockerException
from testcontainers.postgres import PostgresContainer
from testcontainers.redis import RedisContainer

pytest.importorskip("psycopg")
pytest.importorskip("redis")

from forze_postgres.kernel.platform.client import PostgresClient, PostgresConfig
from forze_redis.adapters import RedisSearchResultSnapshotAdapter
from forze_redis.adapters.codecs import RedisKeyCodec
from forze_redis.kernel.platform.client import RedisClient, RedisConfig


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
def redis_container() -> RedisContainer:
    """Valkey/Redis for search result snapshot integration tests (combined with Postgres)."""
    _ensure_docker_available()
    with RedisContainer(image="valkey/valkey:9.0") as redis:
        yield redis


@pytest.fixture(scope="session")
def postgres_container():
    """Starts a Postgres container with PGroonga for testing."""
    with PostgresContainer(
        image="ghcr.io/morzecrew/postgres:18", driver="psycopg"
    ) as postgres:
        yield postgres


@pytest.fixture(scope="session")
def pgvector_container():
    """Starts Postgres with the ``pgvector`` extension (no PGroonga)."""
    with PostgresContainer(
        image="pgvector/pgvector:pg18-trixie",
        driver="psycopg",
    ) as postgres:
        yield postgres


@pytest_asyncio.fixture(scope="function")
async def pg_client(postgres_container):
    """Provides an initialized PostgresClient connected to the test container."""
    url = postgres_container.get_connection_url()

    # testcontainers with driver="psycopg" yields postgresql+psycopg://...
    # which we can replace to standard postgresql:// for psycopg pool connection string
    if url.startswith("postgresql+psycopg://"):
        url = url.replace("postgresql+psycopg://", "postgresql://")

    client = PostgresClient()
    await client.initialize(dsn=url, config=PostgresConfig(min_size=1, max_size=5))

    yield client

    await client.close()


@pytest_asyncio.fixture(scope="function")
async def pgvector_client(pgvector_container):
    """PostgresClient against :func:`pgvector_container` (pgvector preinstalled)."""
    url = pgvector_container.get_connection_url()

    if url.startswith("postgresql+psycopg://"):
        url = url.replace("postgresql+psycopg://", "postgresql://")

    client = PostgresClient()
    await client.initialize(dsn=url, config=PostgresConfig(min_size=1, max_size=5))

    yield client

    await client.close()


# ....................... #
# Valkey/Redis: used with Postgres for :class:`SearchResultSnapshotDepKey` in integration tests.
# ....................... #


@pytest_asyncio.fixture(scope="function")
async def redis_client(redis_container: RedisContainer) -> RedisClient:
    """Initialized :class:`RedisClient` (same pattern as ``tests.integration.test_forze_redis``)."""

    host = redis_container.get_container_host_ip()
    port = redis_container.get_exposed_port(6379)
    dsn = f"redis://{host}:{port}/0"
    client = RedisClient()
    await client.initialize(dsn=dsn, config=RedisConfig(max_size=5))
    yield client
    await client.close()


@pytest_asyncio.fixture(scope="function")
async def redis_search_snapshot(
    redis_client: RedisClient,
) -> RedisSearchResultSnapshotAdapter:
    """Namespaced :class:`RedisSearchResultSnapshotAdapter` for a single test."""

    namespace = f"it:search_snapshot:{uuid4().hex[:12]}"
    return RedisSearchResultSnapshotAdapter(
        client=redis_client,
        key_codec=RedisKeyCodec(namespace=namespace),
    )
