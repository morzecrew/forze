"""Unit tests for :mod:`forze_meilisearch.execution.lifecycle.pool`."""

from unittest.mock import AsyncMock, Mock

import pytest
from pydantic import SecretStr

from forze.application.execution import Deps, LifecyclePlan
from forze_meilisearch.execution.deps.keys import MeilisearchClientDepKey
from forze_meilisearch.execution.lifecycle import (
    MeilisearchShutdownHook,
    MeilisearchStartupHook,
    meilisearch_lifecycle_step,
    routed_meilisearch_lifecycle_step,
)
from forze_meilisearch.kernel.client import MeilisearchClient, MeilisearchConfig
from tests.support.execution_context import context_from_deps


@pytest.mark.asyncio
async def test_meilisearch_startup_hook_initializes_client() -> None:
    client = Mock(spec=MeilisearchClient)
    client.initialize = AsyncMock(return_value=None)
    ctx = context_from_deps(Deps.plain({MeilisearchClientDepKey: client}))
    config = MeilisearchConfig()
    hook = MeilisearchStartupHook(
        url="http://localhost:7700",
        api_key="masterKey",
        config=config,
    )

    await hook(ctx)

    client.initialize.assert_awaited_once_with(
        "http://localhost:7700",
        SecretStr("masterKey"),
        config=config,
    )


@pytest.mark.asyncio
async def test_meilisearch_shutdown_hook_closes_client() -> None:
    client = Mock(spec=MeilisearchClient)
    client.aclose = AsyncMock(return_value=None)
    ctx = context_from_deps(Deps.plain({MeilisearchClientDepKey: client}))
    hook = MeilisearchShutdownHook()

    await hook(ctx)

    client.aclose.assert_awaited_once()


def test_meilisearch_lifecycle_step_builds_hooks() -> None:
    step = meilisearch_lifecycle_step(url="http://localhost:7700", api_key="key")

    assert step.id == "meilisearch_lifecycle"
    assert isinstance(step.startup, MeilisearchStartupHook)
    assert isinstance(step.shutdown, MeilisearchShutdownHook)


class _MockRoutedMeilisearch:
    def __init__(self) -> None:
        self.startup_calls = 0
        self.close_calls = 0

    async def startup(self) -> None:
        self.startup_calls += 1

    async def close(self) -> None:
        self.close_calls += 1


@pytest.mark.asyncio
async def test_routed_meilisearch_lifecycle_step_invokes_client() -> None:
    client = _MockRoutedMeilisearch()
    ctx = context_from_deps(Deps.plain({MeilisearchClientDepKey: client}))
    plan = LifecyclePlan.from_steps(routed_meilisearch_lifecycle_step(client=client))
    frozen = plan.freeze()

    await frozen.startup(ctx)
    await frozen.shutdown(ctx)

    assert client.startup_calls == 1
    assert client.close_calls == 1
