"""Unit tests for forze_neo4j lifecycle hooks."""

from unittest.mock import AsyncMock, Mock

import pytest
from pydantic import SecretStr

from forze.application.execution import Deps
from forze_neo4j.execution.deps import Neo4jClientDepKey
from forze_neo4j.execution.lifecycle import (
    Neo4jShutdownHook,
    Neo4jStartupHook,
    neo4j_lifecycle_step,
)
from forze_neo4j.kernel.client import Neo4jClient, Neo4jConfig
from tests.support.execution_context import context_from_deps


@pytest.mark.asyncio
async def test_startup_hook_initializes_client() -> None:
    client = Mock(spec=Neo4jClient)
    client.initialize = AsyncMock(return_value=None)
    ctx = context_from_deps(Deps.plain({Neo4jClientDepKey: client}))
    config = Neo4jConfig()
    hook = Neo4jStartupHook(uri="neo4j://localhost:7687", auth=("neo4j", "pw"), config=config)

    await hook(ctx)

    client.initialize.assert_awaited_once_with(
        SecretStr("neo4j://localhost:7687"),
        auth=("neo4j", "pw"),
        config=config,
    )


@pytest.mark.asyncio
async def test_shutdown_hook_closes_client() -> None:
    client = Mock(spec=Neo4jClient)
    client.close = AsyncMock(return_value=None)
    ctx = context_from_deps(Deps.plain({Neo4jClientDepKey: client}))

    await Neo4jShutdownHook()(ctx)

    client.close.assert_awaited_once()


def test_lifecycle_step_builds_hooks() -> None:
    step = neo4j_lifecycle_step(uri="neo4j://localhost:7687")
    assert step.id == "neo4j_lifecycle"
    assert isinstance(step.startup, Neo4jStartupHook)
    assert isinstance(step.shutdown, Neo4jShutdownHook)


def test_startup_hook_redacts_uri_in_repr() -> None:
    hook = Neo4jStartupHook(uri="neo4j://secret@host:7687", auth=("u", "p"))
    assert "secret" not in repr(hook)
