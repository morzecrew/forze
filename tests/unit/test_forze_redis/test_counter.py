from datetime import timedelta
from unittest.mock import AsyncMock, Mock
from uuid import UUID

import pytest

from forze.application.contracts.counter import counter_capabilities
from forze.application.contracts.tenancy import TenantIdentity
from forze_redis.adapters.counter import RedisCounterAdapter
from forze_redis.kernel.client import RedisClient


@pytest.fixture
def redis_mock() -> Mock:
    return Mock(spec=RedisClient)


@pytest.mark.asyncio
async def test_redis_counter_adapter_without_tenant(redis_mock: Mock) -> None:
    counter = RedisCounterAdapter(
        client=redis_mock,
        namespace="ns",
    )

    redis_mock.incr = AsyncMock(return_value=1)
    await counter.incr(suffix="my-suffix")
    redis_mock.incr.assert_called_once_with("counter:ns:my-suffix", 1)


@pytest.mark.asyncio
async def test_redis_counter_adapter_with_tenant(redis_mock: Mock) -> None:
    tid = UUID("12345678-1234-5678-1234-567812345678")
    counter = RedisCounterAdapter(
        client=redis_mock,
        namespace="ns",
        tenant_aware=True,
        tenant_provider=lambda: TenantIdentity(tenant_id=tid),
    )

    redis_mock.incr = AsyncMock(return_value=2)
    await counter.incr(suffix="my-suffix")
    redis_mock.incr.assert_called_once_with(
        f"tenant:{tid}:counter:ns:my-suffix",
        1,
    )

    # A decrement is a negative ``INCRBY`` — one path for every step, with or without a ttl.
    await counter.decr(suffix="my-suffix")
    redis_mock.incr.assert_called_with(
        f"tenant:{tid}:counter:ns:my-suffix",
        -1,
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
        namespace="ns",
    )
    redis_mock.incr = AsyncMock(return_value=10)

    batch = await counter.incr_batch(size=3, suffix="b")

    redis_mock.incr.assert_called_once_with("counter:ns:b", 3)
    assert batch == [8, 9, 10]


@pytest.mark.asyncio
async def test_redis_counter_zero_step_reads_without_writing(redis_mock: Mock) -> None:
    counter = RedisCounterAdapter(client=redis_mock, namespace="ns")
    redis_mock.get = AsyncMock(side_effect=[b"7", None])
    redis_mock.incr = AsyncMock()

    assert await counter.incr(0, suffix="r") == 7
    assert await counter.decr(0, suffix="absent") == 0

    redis_mock.incr.assert_not_called()


@pytest.mark.asyncio
async def test_redis_counter_ttl_rounds_up_to_whole_milliseconds(redis_mock: Mock) -> None:
    counter = RedisCounterAdapter(client=redis_mock, namespace="ns")
    redis_mock.run_script = AsyncMock(return_value="1")
    redis_mock.set = AsyncMock(return_value=True)

    assert await counter.incr(suffix="t", ttl=timedelta(microseconds=1)) == 1
    _, keys, args = redis_mock.run_script.call_args.args
    assert keys == ["counter:ns:t"]
    assert args == [1, 1]  # a sub-millisecond lifetime is 1 ms, never 0 (no expiry)

    assert await counter.reset(4, suffix="t", ttl=timedelta(seconds=1.5)) == 4
    redis_mock.set.assert_called_once_with("counter:ns:t", "4", px=1500)


def test_redis_counter_reports_expiry(redis_mock: Mock) -> None:
    assert counter_capabilities(RedisCounterAdapter(client=redis_mock, namespace="ns")).expiry
