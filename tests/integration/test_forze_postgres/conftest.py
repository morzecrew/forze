"""Pytest configuration for forze_postgres integration tests."""

import pytest
import pytest_asyncio
from testcontainers.postgres import PostgresContainer

pytest.importorskip("psycopg")

from forze_postgres.kernel.platform.client import PostgresClient, PostgresConfig


@pytest.fixture(scope="session")
def postgres_container():
    """Starts a Postgres container with PGroonga for testing."""
    with PostgresContainer(
        image="ghcr.io/morzecrew/postgres:18-cron-pgroonga", driver="psycopg"
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
