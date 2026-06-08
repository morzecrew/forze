"""Fixtures for forze_neo4j integration tests."""

import shutil
from collections.abc import AsyncIterator, Iterator

import pytest
import pytest_asyncio

pytest.importorskip("neo4j")
pytest.importorskip("testcontainers.neo4j")

from testcontainers.neo4j import Neo4jContainer  # noqa: E402

from forze_neo4j.kernel.client import Neo4jClient  # noqa: E402


def _ensure_docker() -> None:
    if shutil.which("docker") is None:
        pytest.skip("Docker is required for Neo4j integration tests")


@pytest.fixture(scope="session")
def neo4j_container() -> Iterator[Neo4jContainer]:
    """Start a Neo4j container for the test session."""

    _ensure_docker()

    with Neo4jContainer(image="neo4j:5.26") as container:
        yield container


@pytest_asyncio.fixture
async def neo4j_client(neo4j_container: Neo4jContainer) -> AsyncIterator[Neo4jClient]:
    """Provide an initialized client; wipe the database before each test."""

    client = Neo4jClient()
    await client.initialize(
        neo4j_container.get_connection_url(),
        auth=(neo4j_container.username, neo4j_container.password),
    )

    await client.run("MATCH (n) DETACH DELETE n")

    yield client

    await client.close()
