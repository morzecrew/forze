"""Unit tests for :mod:`forze_mongo.execution.lifecycle.pool`."""

from unittest.mock import AsyncMock, Mock

import pytest
from pydantic import SecretStr

from forze.application.execution import Deps, LifecyclePlan
from tests.support.execution_context import context_from_deps
from forze_mongo.execution.deps import MongoClientDepKey
from forze_mongo.execution.lifecycle import (
    MongoShutdownHook,
    MongoStartupHook,
    mongo_lifecycle_step,
    routed_mongo_lifecycle_step,
)
from forze_mongo.kernel.client import MongoClient, MongoConfig


@pytest.mark.asyncio
async def test_mongo_startup_hook_initializes_client() -> None:
    client = Mock(spec=MongoClient)
    client.initialize = AsyncMock(return_value=None)
    ctx = context_from_deps(Deps.plain({MongoClientDepKey: client}))
    config = MongoConfig()
    hook = MongoStartupHook(
        uri="mongodb://localhost:27017",
        db_name="testdb",
        config=config,
    )

    await hook(ctx)

    client.initialize.assert_awaited_once_with(
        SecretStr("mongodb://localhost:27017"),
        db_name="testdb",
        config=config,
    )


@pytest.mark.asyncio
async def test_mongo_shutdown_hook_closes_client() -> None:
    client = Mock(spec=MongoClient)
    client.close = AsyncMock(return_value=None)
    ctx = context_from_deps(Deps.plain({MongoClientDepKey: client}))
    hook = MongoShutdownHook()

    await hook(ctx)

    client.close.assert_awaited_once()


def test_mongo_lifecycle_step_builds_hooks() -> None:
    step = mongo_lifecycle_step(uri="mongodb://localhost:27017", db_name="testdb")

    assert step.id == "mongo_lifecycle"
    assert isinstance(step.startup, MongoStartupHook)
    assert isinstance(step.shutdown, MongoShutdownHook)


class _MockRoutedMongo:
    def __init__(self) -> None:
        self.startup_calls = 0
        self.close_calls = 0

    async def startup(self) -> None:
        self.startup_calls += 1

    async def close(self) -> None:
        self.close_calls += 1


@pytest.mark.asyncio
async def test_routed_mongo_lifecycle_step_invokes_client() -> None:
    client = _MockRoutedMongo()
    ctx = context_from_deps(Deps.plain({MongoClientDepKey: client}))
    plan = LifecyclePlan.from_steps(routed_mongo_lifecycle_step(client=client))
    frozen = plan.freeze()

    await frozen.startup(ctx)
    await frozen.shutdown(ctx)

    assert client.startup_calls == 1
    assert client.close_calls == 1
