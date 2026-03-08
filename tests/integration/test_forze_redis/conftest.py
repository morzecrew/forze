"""Pytest configuration for forze_redis integration tests."""

from uuid import uuid4

import pytest
import pytest_asyncio
from docker import from_env
from docker.errors import DockerException
from pydantic import BaseModel
from testcontainers.redis import RedisContainer

pytest.importorskip("redis")

from forze.utils.codecs import KeyCodec
from forze_redis.adapters import (
    RedisCacheAdapter,
    RedisCounterAdapter,
    RedisIdempotencyAdapter,
    RedisStreamAdapter,
    RedisStreamCodec,
    RedisStreamGroupAdapter,
)
from forze_redis.kernel.platform.client import RedisClient, RedisConfig


def _ensure_docker_available() -> None:
    client = None

    try:
        client = from_env()
        client.ping()
    except DockerException as exc:
        pytest.skip(f"Docker is required for Redis integration tests: {exc}")
    finally:
        if client is not None:
            client.close()


@pytest.fixture(scope="session")
def redis_container() -> RedisContainer:
    """Start a Redis container for integration tests."""
    _ensure_docker_available()

    with RedisContainer(image="valkey/valkey:9.0") as redis:
        yield redis


@pytest_asyncio.fixture(scope="function")
async def redis_client(redis_container: RedisContainer) -> RedisClient:
    """Provide an initialized RedisClient connected to test container."""

    host = redis_container.get_container_host_ip()
    port = redis_container.get_exposed_port(6379)
    dsn = f"redis://{host}:{port}/0"

    client = RedisClient()
    await client.initialize(dsn=dsn, config=RedisConfig(max_size=5))

    yield client

    await client.close()


@pytest_asyncio.fixture(scope="function")
async def redis_cache(redis_client: RedisClient) -> RedisCacheAdapter:
    """Provide a RedisCacheAdapter with a unique namespace per test."""
    namespace = f"it:cache:{uuid4().hex[:12]}"
    return RedisCacheAdapter(
        client=redis_client,
        key_codec=KeyCodec(namespace=namespace),
    )


@pytest_asyncio.fixture(scope="function")
async def redis_counter(redis_client: RedisClient) -> RedisCounterAdapter:
    """Provide a RedisCounterAdapter with a unique namespace per test."""
    namespace = f"it:counter:{uuid4().hex[:12]}"
    return RedisCounterAdapter(
        client=redis_client,
        key_codec=KeyCodec(namespace=namespace),
        tenant_context=None,
    )


@pytest_asyncio.fixture(scope="function")
async def redis_idempotency(redis_client: RedisClient) -> RedisIdempotencyAdapter:
    """Provide a RedisIdempotencyAdapter for integration tests."""
    return RedisIdempotencyAdapter(client=redis_client)


class _StreamPayload(BaseModel):
    """Minimal payload model for stream integration tests."""

    value: str


@pytest_asyncio.fixture(scope="function")
async def redis_stream(redis_client: RedisClient) -> RedisStreamAdapter[_StreamPayload]:
    """Provide a RedisStreamAdapter for integration tests."""
    codec = RedisStreamCodec(model=_StreamPayload)
    return RedisStreamAdapter(client=redis_client, codec=codec)


@pytest_asyncio.fixture(scope="function")
async def redis_stream_group(
    redis_client: RedisClient,
) -> RedisStreamGroupAdapter[_StreamPayload]:
    """Provide a RedisStreamGroupAdapter for integration tests."""
    codec = RedisStreamCodec(model=_StreamPayload)
    return RedisStreamGroupAdapter(client=redis_client, codec=codec)


@pytest.fixture(scope="function")
def stream_payload_cls() -> type[_StreamPayload]:
    """Provide the stream payload model for constructing test messages."""
    return _StreamPayload
