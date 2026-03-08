"""Pytest configuration for forze_mongo integration tests."""

import shutil
from uuid import uuid4

import pytest
import pytest_asyncio

pytest.importorskip("pymongo")
pytest.importorskip("testcontainers.mongodb")

from testcontainers.mongodb import MongoDbContainer

from forze_mongo.kernel.platform import MongoClient


@pytest.fixture(scope="session")
def mongo_container() -> MongoDbContainer:
    """Start a MongoDB container for integration tests."""
    if shutil.which("docker") is None:
        pytest.skip("Docker is required for Mongo integration tests")

    with MongoDbContainer(image="mongo:7.0") as mongo:
        yield mongo


@pytest_asyncio.fixture(scope="function")
async def mongo_client(mongo_container: MongoDbContainer) -> MongoClient:
    """Provide an initialized Mongo client connected to test container."""
    uri = mongo_container.get_connection_url()
    db_name = f"forze_test_{uuid4().hex[:8]}"

    client = MongoClient()
    await client.initialize(uri, db_name=db_name)

    yield client

    await client.close()
