"""Both a Postgres and a Mongo backend, in one process — the fixture shape a portability
round-trip needs (RFC 0017 §8: two backends wired together, export from one, import into the
other)."""

from collections.abc import AsyncIterator, Iterator
from uuid import uuid4

import pytest
import pytest_asyncio

pytest.importorskip("psycopg")
pytest.importorskip("pymongo")
pytest.importorskip("testcontainers.mongodb")

from testcontainers.mongodb import MongoDbContainer
from testcontainers.postgres import PostgresContainer

from forze_mongo.kernel.client import MongoClient
from forze_postgres.kernel.client.client import PostgresClient, PostgresConfig
from tests.support.docker import ensure_docker_available

# ----------------------- #


@pytest.fixture(scope="session")
def portability_postgres_container() -> Iterator[PostgresContainer]:
    ensure_docker_available()

    with PostgresContainer(image="ghcr.io/morzecrew/postgres:18", driver="psycopg") as postgres:
        yield postgres


@pytest.fixture(scope="session")
def portability_mongo_container() -> Iterator[MongoDbContainer]:
    ensure_docker_available()

    with MongoDbContainer(image="mongo:8.0-noble") as mongo:
        yield mongo


# ....................... #


@pytest_asyncio.fixture(scope="function")
async def pg_client(
    portability_postgres_container: PostgresContainer,
) -> AsyncIterator[PostgresClient]:
    url = portability_postgres_container.get_connection_url()

    if url.startswith("postgresql+psycopg://"):
        url = url.replace("postgresql+psycopg://", "postgresql://")

    client = PostgresClient()
    await client.initialize(dsn=url, config=PostgresConfig(min_size=1, max_size=5))

    yield client

    await client.close()


@pytest_asyncio.fixture(scope="function")
async def mongo_client(portability_mongo_container: MongoDbContainer) -> AsyncIterator[MongoClient]:
    uri = portability_mongo_container.get_connection_url()
    db_name = f"forze_portability_{uuid4().hex[:8]}"

    client = MongoClient()
    await client.initialize(uri, db_name=db_name)

    yield client

    await client.close()
