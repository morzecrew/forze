"""Unit tests for :mod:`forze_inngest.execution.lifecycle.pool`."""

import pytest

from forze.application.execution import Deps, LifecyclePlan
from tests.support.execution_context import context_from_deps
from forze_inngest.execution.deps import InngestClientDepKey
from forze_inngest.execution.lifecycle import (
    InngestShutdownHook,
    InngestStartupHook,
    inngest_lifecycle_step,
    routed_inngest_lifecycle_step,
)


@pytest.mark.asyncio
async def test_inngest_startup_hook_resolves_client() -> None:
    client = object()
    ctx = context_from_deps(Deps.plain({InngestClientDepKey: client}))
    hook = InngestStartupHook()

    await hook(ctx)


@pytest.mark.asyncio
async def test_inngest_shutdown_hook_is_noop() -> None:
    ctx = context_from_deps(Deps.plain({InngestClientDepKey: object()}))
    hook = InngestShutdownHook()

    await hook(ctx)


def test_inngest_lifecycle_step_builds_hooks() -> None:
    step = inngest_lifecycle_step()

    assert step.id == "inngest_lifecycle"
    assert isinstance(step.startup, InngestStartupHook)
    assert isinstance(step.shutdown, InngestShutdownHook)


class _MockRoutedInngest:
    def __init__(self) -> None:
        self.startup_calls = 0
        self.close_calls = 0

    async def startup(self) -> None:
        self.startup_calls += 1

    async def close(self) -> None:
        self.close_calls += 1


@pytest.mark.asyncio
async def test_routed_inngest_lifecycle_step_invokes_client() -> None:
    client = _MockRoutedInngest()
    ctx = context_from_deps(Deps.plain({InngestClientDepKey: client}))
    plan = LifecyclePlan.from_steps(routed_inngest_lifecycle_step(client=client))
    frozen = plan.freeze()

    await frozen.startup(ctx)
    await frozen.shutdown(ctx)

    assert client.startup_calls == 1
    assert client.close_calls == 1
