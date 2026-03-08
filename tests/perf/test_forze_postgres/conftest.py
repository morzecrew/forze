"""Pytest configuration for forze_postgres performance tests."""

import pytest
import pytest_asyncio
from testcontainers.postgres import PostgresContainer

pytest.importorskip("psycopg")

from forze_postgres.kernel.platform.client import PostgresClient, PostgresConfig


@pytest.fixture(scope="session")
def postgres_container():
    """Starts a Postgres container with PGroonga for performance testing."""
    with PostgresContainer(
        image="ghcr.io/morzecrew/postgres:18-cron-pgroonga", driver="psycopg"
    ) as postgres:
        yield postgres


@pytest_asyncio.fixture(scope="function")
async def pg_client(postgres_container):
    """Provides an initialized PostgresClient connected to the test container."""
    url = postgres_container.get_connection_url()

    if url.startswith("postgresql+psycopg://"):
        url = url.replace("postgresql+psycopg://", "postgresql://")

    client = PostgresClient()
    await client.initialize(dsn=url, config=PostgresConfig(min_size=2, max_size=10))

    yield client

    await client.close()
