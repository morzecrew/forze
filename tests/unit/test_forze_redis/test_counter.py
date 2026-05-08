from unittest.mock import AsyncMock, Mock
from uuid import UUID

import pytest

from forze_redis.adapters.codecs import RedisKeyCodec
from forze.application.contracts.tenancy import TenantIdentity

from forze_redis.adapters.counter import RedisCounterAdapter
from forze_redis.kernel.platform.client import RedisClient


@pytest.fixture
def redis_mock() -> Mock:
    return Mock(spec=RedisClient)


@pytest.mark.asyncio
async def test_redis_counter_adapter_without_tenant(redis_mock: Mock) -> None:
    counter = RedisCounterAdapter(
        client=redis_mock,
        key_codec=RedisKeyCodec(namespace="ns"),
    )

    redis_mock.incr = AsyncMock(return_value=1)
    await counter.incr(suffix="my-suffix")
    redis_mock.incr.assert_called_once_with("counter:ns:my-suffix", 1)


@pytest.mark.asyncio
async def test_redis_counter_adapter_with_tenant(redis_mock: Mock) -> None:
    tid = UUID("12345678-1234-5678-1234-567812345678")
    counter = RedisCounterAdapter(
        client=redis_mock,
        key_codec=RedisKeyCodec(namespace="ns"),
        tenant_aware=True,
        tenant_provider=lambda: TenantIdentity(tenant_id=tid),
    )

    redis_mock.incr = AsyncMock(return_value=2)
    await counter.incr(suffix="my-suffix")
    redis_mock.incr.assert_called_once_with(
        f"tenant:{tid}:counter:ns:my-suffix",
        1,
    )

    redis_mock.decr = AsyncMock(return_value=1)
    await counter.decr(suffix="my-suffix")
    redis_mock.decr.assert_called_once_with(
        f"tenant:{tid}:counter:ns:my-suffix",
        1,
    )

    redis_mock.reset = AsyncMock(return_value=0)
    await counter.reset(value=0, suffix="my-suffix")
    redis_mock.reset.assert_called_once_with(
        f"tenant:{tid}:counter:ns:my-suffix",
        0,
    )


@pytest.mark.asyncio
async def test_redis_counter_incr_batch_returns_range(redis_mock: Mock) -> None:
    counter = RedisCounterAdapter(
        client=redis_mock,
        key_codec=RedisKeyCodec(namespace="ns"),
    )
    redis_mock.incr = AsyncMock(return_value=10)

    batch = await counter.incr_batch(size=3, suffix="b")

    redis_mock.incr.assert_called_once_with("counter:ns:b", 3)
    assert batch == [8, 9, 10]
