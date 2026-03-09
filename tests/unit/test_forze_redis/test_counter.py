from unittest.mock import AsyncMock, Mock

import pytest

from forze.application.contracts.tenant import TenantContextPort
from forze.base.codecs import KeyCodec
from forze_redis.adapters.counter import RedisCounterAdapter
from forze_redis.kernel.platform.client import RedisClient


@pytest.fixture
def redis_mock():
    return Mock(spec=RedisClient)


@pytest.fixture
def tenant_mock():
    mock = Mock(spec=TenantContextPort)
    mock.get.return_value = "tenant-123"
    return mock


@pytest.mark.asyncio
async def test_redis_counter_adapter_without_tenant(redis_mock):
    counter = RedisCounterAdapter(
        client=redis_mock, key_codec=KeyCodec(namespace="ns")
    )

    redis_mock.incr = AsyncMock(return_value=1)
    await counter.incr(suffix="my-suffix")
    redis_mock.incr.assert_called_once_with("ns:my-suffix", 1)


@pytest.mark.asyncio
async def test_redis_counter_adapter_with_tenant(redis_mock, tenant_mock):
    counter = RedisCounterAdapter(
        client=redis_mock,
        key_codec=KeyCodec(namespace="ns"),
        tenant_context=tenant_mock,
    )

    redis_mock.incr = AsyncMock(return_value=2)
    await counter.incr(suffix="my-suffix")
    redis_mock.incr.assert_called_once_with("ns:tenant-123:my-suffix", 1)

    redis_mock.decr = AsyncMock(return_value=1)
    await counter.decr(suffix="my-suffix")
    redis_mock.decr.assert_called_once_with("ns:tenant-123:my-suffix", 1)

    redis_mock.reset = AsyncMock(return_value=0)
    await counter.reset(value=0, suffix="my-suffix")
    redis_mock.reset.assert_called_once_with("ns:tenant-123:my-suffix", 0)
