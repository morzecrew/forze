"""Unit tests for :mod:`forze_clickhouse.execution.lifecycle.pool`."""

from unittest.mock import AsyncMock, Mock

import pytest

from forze.application.execution import Deps, LifecyclePlan
from tests.support.execution_context import context_from_deps
from forze_clickhouse.execution.deps import ClickHouseClientDepKey
from forze_clickhouse.execution.lifecycle import (
    ClickHouseShutdownHook,
    ClickHouseStartupHook,
    clickhouse_lifecycle_step,
    routed_clickhouse_lifecycle_step,
)
from forze_clickhouse.kernel.client import ClickHouseClient, ClickHouseConfig


@pytest.mark.asyncio
async def test_clickhouse_startup_hook_initializes_client() -> None:
    client = Mock(spec=ClickHouseClient)
    client.initialize = AsyncMock(return_value=None)
    ctx = context_from_deps(Deps.plain({ClickHouseClientDepKey: client}))
    connection = ClickHouseConfig(host="localhost", port=8123)
    hook = ClickHouseStartupHook(connection=connection)

    await hook(ctx)

    client.initialize.assert_awaited_once_with(connection)


@pytest.mark.asyncio
async def test_clickhouse_shutdown_hook_closes_client() -> None:
    client = Mock(spec=ClickHouseClient)
    client.close = AsyncMock(return_value=None)
    ctx = context_from_deps(Deps.plain({ClickHouseClientDepKey: client}))
    hook = ClickHouseShutdownHook()

    await hook(ctx)

    client.close.assert_awaited_once()


def test_clickhouse_lifecycle_step_builds_hooks() -> None:
    connection = ClickHouseConfig()
    step = clickhouse_lifecycle_step(connection=connection)

    assert step.id == "clickhouse_lifecycle"
    assert isinstance(step.startup, ClickHouseStartupHook)
    assert isinstance(step.shutdown, ClickHouseShutdownHook)


class _MockRoutedClickHouse:
    def __init__(self) -> None:
        self.startup_calls = 0
        self.close_calls = 0

    async def startup(self) -> None:
        self.startup_calls += 1

    async def close(self) -> None:
        self.close_calls += 1


@pytest.mark.asyncio
async def test_routed_clickhouse_lifecycle_step_invokes_client() -> None:
    client = _MockRoutedClickHouse()
    ctx = context_from_deps(Deps.plain({ClickHouseClientDepKey: client}))
    plan = LifecyclePlan.from_steps(routed_clickhouse_lifecycle_step(client=client))
    frozen = plan.freeze()

    await frozen.startup(ctx)
    await frozen.shutdown(ctx)

    assert client.startup_calls == 1
    assert client.close_calls == 1
