"""Unit tests for :mod:`forze_gcs.execution.lifecycle.pool`."""

from unittest.mock import AsyncMock, Mock

import pytest

from forze.application.execution import Deps, LifecyclePlan
from tests.support.execution_context import context_from_deps
from forze_gcs.execution.deps import GCSClientDepKey
from forze_gcs.execution.lifecycle import (
    GCSShutdownHook,
    GCSStartupHook,
    gcs_lifecycle_step,
    routed_gcs_lifecycle_step,
)
from forze_gcs.kernel.client import GCSClient, GCSConfig


@pytest.mark.asyncio
async def test_gcs_startup_hook_initializes_client() -> None:
    client = Mock(spec=GCSClient)
    client.initialize = AsyncMock(return_value=None)
    ctx = context_from_deps(Deps.plain({GCSClientDepKey: client}))
    config = GCSConfig()
    hook = GCSStartupHook(
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
async def test_gcs_shutdown_hook_closes_client() -> None:
    client = Mock(spec=GCSClient)
    client.close = AsyncMock(return_value=None)
    ctx = context_from_deps(Deps.plain({GCSClientDepKey: client}))
    hook = GCSShutdownHook()

    await hook(ctx)

    client.close.assert_awaited_once()


def test_gcs_lifecycle_step_builds_hooks() -> None:
    step = gcs_lifecycle_step(project_id="my-project")

    assert step.id == "gcs_lifecycle"
    assert isinstance(step.startup, GCSStartupHook)
    assert isinstance(step.shutdown, GCSShutdownHook)


class _MockRoutedGCS:
    def __init__(self) -> None:
        self.startup_calls = 0
        self.close_calls = 0

    async def startup(self) -> None:
        self.startup_calls += 1

    async def close(self) -> None:
        self.close_calls += 1


@pytest.mark.asyncio
async def test_routed_gcs_lifecycle_step_invokes_client() -> None:
    client = _MockRoutedGCS()
    ctx = context_from_deps(Deps.plain({GCSClientDepKey: client}))
    plan = LifecyclePlan.from_steps(routed_gcs_lifecycle_step(client=client))
    frozen = plan.freeze()

    await frozen.startup(ctx)
    await frozen.shutdown(ctx)

    assert client.startup_calls == 1
    assert client.close_calls == 1
