"""Pytest configuration for forze_mongo integration tests."""

import shutil
import time
from uuid import uuid4

import pytest
import pytest_asyncio

pytest.importorskip("pymongo")
pytest.importorskip("testcontainers.mongodb")

from docker import from_env
from docker.errors import DockerException
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs
from testcontainers.mongodb import MongoDbContainer
from testcontainers.redis import RedisContainer

from forze_mongo.kernel.platform import MongoClient
from forze_redis.kernel.client import RedisClient, RedisConfig


def _ensure_docker() -> None:
    if shutil.which("docker") is None:
        pytest.skip("Docker is required for Mongo integration tests")


@pytest.fixture(scope="session")
def mongo_container() -> MongoDbContainer:
    """Start a MongoDB container for integration tests."""

    _ensure_docker()

    with MongoDbContainer(image="mongo:8.0-noble") as mongo:
        yield mongo


@pytest.fixture(scope="session")
def mongo_replica_container() -> DockerContainer:
    """Start a MongoDB replica set container for transaction tests."""

    _ensure_docker()

    mongo = (
        DockerContainer(image="mongo:8.0-noble")
        .with_command(["--replSet", "rs0"])
        .with_bind_ports(27017, 27017)
    )
    mongo.start()

    def ready(text: str) -> bool:
        return "waiting for connections" in text.lower()

    wait_for_logs(mongo, ready)

    # Use localhost:27017 so both the node and the client use the same address
    host = "localhost"
    port = 27017
    init_cmd = [
        "mongosh",
        "--quiet",
        "--eval",
        f"rs.initiate({{_id:'rs0',members:[{{_id:0,host:'{host}:{port}'}}]}})",
    ]
    result = mongo.exec(init_cmd)
    if result.exit_code != 0:
        mongo.stop()
        raise RuntimeError(
            f"Replica set init failed: exit_code={result.exit_code}, "
            f"output={result.output.decode()!r}"
        )

    yield mongo

    mongo.stop()


@pytest_asyncio.fixture(scope="function")
async def mongo_client(mongo_container: MongoDbContainer) -> MongoClient:
    """Provide an initialized Mongo client connected to test container."""
    uri = mongo_container.get_connection_url()
    db_name = f"forze_test_{uuid4().hex[:8]}"

    client = MongoClient()
    await client.initialize(uri, db_name=db_name)

    yield client

    await client.close()


@pytest_asyncio.fixture(scope="function")
async def mongo_client_replica(mongo_replica_container: DockerContainer) -> MongoClient:
    """Provide an initialized Mongo client connected to replica set (for transactions)."""
    uri = "mongodb://localhost:27017/?replicaSet=rs0"
    db_name = f"forze_test_{uuid4().hex[:8]}"

    client = MongoClient()
    await client.initialize(uri, db_name=db_name)

    yield client

    await client.close()


def _wait_container_healthy(
    container: DockerContainer, *, timeout_s: float = 300.0
) -> None:
    """Block until the Docker healthcheck reports ``healthy``."""

    docker = from_env()
    cid = container._container.id
    deadline = time.monotonic() + timeout_s

    while time.monotonic() < deadline:
        try:
            state = docker.containers.get(cid).attrs.get("State", {})
            health = state.get("Health")

            if health and health.get("Status") == "healthy":
                return

        except DockerException:
            pass

        time.sleep(2)

    raise TimeoutError("Container did not become healthy in time")


@pytest.fixture(scope="session")
def mongo_atlas_local_container() -> DockerContainer:
    """MongoDB Atlas Local (mongod + mongot) for ``$search`` / ``$vectorSearch`` tests."""

    _ensure_docker()

    container = (
        DockerContainer("mongodb/mongodb-atlas-local:latest")
        .with_exposed_ports(27017)
        .with_env("DO_NOT_TRACK", "1")
    )
    container.start()

    try:
        _wait_container_healthy(container)
        yield container
    finally:
        container.stop()


@pytest_asyncio.fixture(scope="function")
async def mongo_atlas_client(
    mongo_atlas_local_container: DockerContainer,
) -> MongoClient:
    """Mongo client connected to Atlas Local (``directConnection=true``)."""

    host = mongo_atlas_local_container.get_container_host_ip()
    port = mongo_atlas_local_container.get_exposed_port(27017)
    uri = f"mongodb://{host}:{port}/?directConnection=true"
    db_name = f"forze_atlas_{uuid4().hex[:8]}"

    client = MongoClient()
    await client.initialize(uri, db_name=db_name)

    yield client

    await client.close()


@pytest.fixture(scope="session")
def redis_container() -> RedisContainer:
    """Valkey/Redis for search result snapshot integration tests."""

    _ensure_docker()

    with RedisContainer(image="valkey/valkey:9.0") as redis:
        yield redis


@pytest_asyncio.fixture(scope="function")
async def redis_client(redis_container: RedisContainer) -> RedisClient:
    """Initialized :class:`RedisClient` for snapshot tests."""

    host = redis_container.get_container_host_ip()
    port = redis_container.get_exposed_port(6379)
    dsn = f"redis://{host}:{port}/0"
    client = RedisClient()
    await client.initialize(dsn=dsn, config=RedisConfig(max_size=5))

    yield client

    await client.close()
