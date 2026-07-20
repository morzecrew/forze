"""Unit tests for :mod:`forze_redis.execution.lifecycle.pool`."""

from unittest.mock import AsyncMock, Mock

import pytest
from pydantic import SecretStr

from forze.application.execution import Deps, LifecyclePlan
from forze_redis.execution.deps import RedisClientDepKey
from forze_redis.execution.lifecycle import (
    RedisShutdownHook,
    RedisStartupHook,
    redis_lifecycle_step,
    routed_redis_lifecycle_step,
)
from forze_redis.kernel.client import RedisClient, RedisConfig
from tests.support.execution_context import context_from_deps


@pytest.mark.asyncio
async def test_redis_startup_hook_initializes_client() -> None:
    client = Mock(spec=RedisClient)
    client.initialize = AsyncMock(return_value=None)
    ctx = context_from_deps(Deps.plain({RedisClientDepKey: client}))
    config = RedisConfig()
    hook = RedisStartupHook(dsn="redis://localhost:6379/0", config=config)

    await hook(ctx)

    client.initialize.assert_awaited_once_with(
        SecretStr("redis://localhost:6379/0"),
        config=config,
    )


@pytest.mark.asyncio
async def test_redis_shutdown_hook_closes_client() -> None:
    client = Mock(spec=RedisClient)
    client.close = AsyncMock(return_value=None)
    ctx = context_from_deps(Deps.plain({RedisClientDepKey: client}))
    hook = RedisShutdownHook()

    await hook(ctx)

    client.close.assert_awaited_once()


def test_redis_lifecycle_step_builds_hooks() -> None:
    step = redis_lifecycle_step(dsn="redis://localhost:6379/0")

    assert step.id == "redis_lifecycle"
    assert isinstance(step.startup, RedisStartupHook)
    assert isinstance(step.shutdown, RedisShutdownHook)


class _MockRoutedRedis:
    def __init__(self) -> None:
        self.startup_calls = 0
        self.close_calls = 0

    async def startup(self) -> None:
        self.startup_calls += 1

    async def close(self) -> None:
        self.close_calls += 1


@pytest.mark.asyncio
async def test_routed_redis_lifecycle_step_invokes_client() -> None:
    client = _MockRoutedRedis()
    ctx = context_from_deps(Deps.plain({RedisClientDepKey: client}))
    plan = LifecyclePlan.from_steps(routed_redis_lifecycle_step(client=client))
    frozen = plan.freeze()

    await frozen.startup(ctx)
    await frozen.shutdown(ctx)

    assert client.startup_calls == 1
    assert client.close_calls == 1
