"""Unit tests for Postgres startup/shutdown hooks in :mod:`forze_postgres.execution.lifecycle.pool`."""

from unittest.mock import AsyncMock, Mock

import pytest
from pydantic import SecretStr

from forze.application.execution import Deps, LifecyclePlan
from tests.support.execution_context import context_from_deps
from forze_postgres.execution.deps import PostgresClientDepKey
from forze_postgres.execution.lifecycle.pool import (
    PostgresShutdownHook,
    PostgresStartupHook,
    postgres_lifecycle_step,
    routed_postgres_lifecycle_step,
)
from forze_postgres.kernel.client import PostgresClient, PostgresConfig


@pytest.mark.asyncio
async def test_postgres_startup_hook_initializes_client() -> None:
    client = Mock(spec=PostgresClient)
    client.initialize = AsyncMock(return_value=None)
    ctx = context_from_deps(Deps.plain({PostgresClientDepKey: client}))
    config = PostgresConfig(min_size=1, max_size=2)
    hook = PostgresStartupHook(
        dsn="postgresql://u:p@localhost/db",
        config=config,
    )

    await hook(ctx)

    client.initialize.assert_awaited_once_with(
        SecretStr("postgresql://u:p@localhost/db"),
        config=config,
    )


@pytest.mark.asyncio
async def test_postgres_shutdown_hook_closes_client() -> None:
    client = Mock(spec=PostgresClient)
    client.close = AsyncMock(return_value=None)
    ctx = context_from_deps(Deps.plain({PostgresClientDepKey: client}))
    hook = PostgresShutdownHook()

    await hook(ctx)

    client.close.assert_awaited_once()


def test_postgres_lifecycle_step_builds_hooks() -> None:
    step = postgres_lifecycle_step(dsn="postgresql://u:p@localhost/db")

    assert step.id == "postgres_lifecycle"
    assert isinstance(step.startup, PostgresStartupHook)
    assert isinstance(step.shutdown, PostgresShutdownHook)


class _MockRoutedPostgres:
    def __init__(self) -> None:
        self.startup_calls = 0
        self.close_calls = 0

    async def startup(self) -> None:
        self.startup_calls += 1

    async def close(self) -> None:
        self.close_calls += 1


@pytest.mark.asyncio
async def test_routed_postgres_lifecycle_step_invokes_client() -> None:
    client = _MockRoutedPostgres()
    ctx = context_from_deps(Deps.plain({PostgresClientDepKey: client}))
    plan = LifecyclePlan.from_steps(routed_postgres_lifecycle_step(client=client))
    frozen = plan.freeze()

    await frozen.startup(ctx)
    await frozen.shutdown(ctx)

    assert client.startup_calls == 1
    assert client.close_calls == 1
