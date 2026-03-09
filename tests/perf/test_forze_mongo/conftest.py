"""Pytest configuration for forze_mongo performance tests."""

import shutil
from uuid import uuid4

import pytest
import pytest_asyncio

pytest.importorskip("pymongo")
pytest.importorskip("testcontainers.mongodb")

from testcontainers.mongodb import MongoDbContainer

from forze_mongo.kernel.platform import MongoClient


def _ensure_docker() -> None:
    if shutil.which("docker") is None:
        pytest.skip("Docker is required for Mongo performance tests")


@pytest.fixture(scope="session")
def mongo_container() -> MongoDbContainer:
    """Start a MongoDB container for performance testing."""
    _ensure_docker()

    with MongoDbContainer(image="mongo:8.0-noble") as mongo:
        yield mongo


@pytest_asyncio.fixture(scope="function")
async def mongo_client(mongo_container: MongoDbContainer) -> MongoClient:
    """Provide an initialized Mongo client connected to test container."""
    uri = mongo_container.get_connection_url()
    db_name = f"forze_perf_{uuid4().hex[:8]}"

    client = MongoClient()
    await client.initialize(uri, db_name=db_name)

    yield client

    await client.close()
