"""Pytest configuration for ``forze_authn`` integration tests."""

import pytest
import pytest_asyncio
from docker import from_env
from docker.errors import DockerException
from testcontainers.postgres import PostgresContainer

pytest.importorskip("psycopg")

from forze_postgres.kernel.platform.client import PostgresClient, PostgresConfig


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
    with PostgresContainer(
        image="ghcr.io/morzecrew/postgres:18-cron-pgroonga", driver="psycopg"
    ) as postgres:
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
