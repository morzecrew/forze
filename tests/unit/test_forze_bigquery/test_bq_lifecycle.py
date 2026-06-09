"""Unit tests for :mod:`forze_bigquery.execution.lifecycle.pool`."""

from unittest.mock import AsyncMock, Mock

import pytest

from forze.application.execution import Deps, LifecyclePlan
from tests.support.execution_context import context_from_deps
from forze_bigquery.execution.deps import BigQueryClientDepKey
from forze_bigquery.execution.lifecycle import (
    BigQueryShutdownHook,
    BigQueryStartupHook,
    bigquery_lifecycle_step,
    routed_bigquery_lifecycle_step,
)
from forze_bigquery.kernel.client import BigQueryClient, BigQueryConfig


@pytest.mark.asyncio
async def test_bigquery_startup_hook_initializes_client() -> None:
    client = Mock(spec=BigQueryClient)
    client.initialize = AsyncMock(return_value=None)
    ctx = context_from_deps(Deps.plain({BigQueryClientDepKey: client}))
    config = BigQueryConfig()
    hook = BigQueryStartupHook(
        project_id="my-project",
        service_file="/path/to/key.json",
        config=config,
    )

    await hook(ctx)

    client.initialize.assert_awaited_once_with(
        "my-project",
        service_file="/path/to/key.json",
        config=config,
    )


@pytest.mark.asyncio
async def test_bigquery_shutdown_hook_closes_client() -> None:
    client = Mock(spec=BigQueryClient)
    client.close = AsyncMock(return_value=None)
    ctx = context_from_deps(Deps.plain({BigQueryClientDepKey: client}))
    hook = BigQueryShutdownHook()

    await hook(ctx)

    client.close.assert_awaited_once()


def test_bigquery_lifecycle_step_builds_hooks() -> None:
    step = bigquery_lifecycle_step(project_id="my-project")

    assert step.id == "bigquery_lifecycle"
    assert isinstance(step.startup, BigQueryStartupHook)
    assert isinstance(step.shutdown, BigQueryShutdownHook)


class _MockRoutedBigQuery:
    def __init__(self) -> None:
        self.startup_calls = 0
        self.close_calls = 0

    async def startup(self) -> None:
        self.startup_calls += 1

    async def close(self) -> None:
        self.close_calls += 1


@pytest.mark.asyncio
async def test_routed_bigquery_lifecycle_step_invokes_client() -> None:
    client = _MockRoutedBigQuery()
    ctx = context_from_deps(Deps.plain({BigQueryClientDepKey: client}))
    plan = LifecyclePlan.from_steps(routed_bigquery_lifecycle_step(client=client))
    frozen = plan.freeze()

    await frozen.startup(ctx)
    await frozen.shutdown(ctx)

    assert client.startup_calls == 1
    assert client.close_calls == 1
