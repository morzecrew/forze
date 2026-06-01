"""Unit tests for :mod:`forze_firestore.execution.lifecycle.pool`."""

from unittest.mock import AsyncMock, Mock

import pytest

from forze.application.execution import Deps, LifecyclePlan
from tests.support.execution_context import context_from_deps
from forze_firestore.execution.deps import FirestoreClientDepKey
from forze_firestore.execution.lifecycle import (
    FirestoreShutdownHook,
    FirestoreStartupHook,
    firestore_lifecycle_step,
    routed_firestore_lifecycle_step,
)
from forze_firestore.kernel.client import FirestoreClient


@pytest.mark.asyncio
async def test_firestore_startup_hook_initializes_client() -> None:
    client = Mock(spec=FirestoreClient)
    client.initialize = AsyncMock(return_value=None)
    ctx = context_from_deps(Deps.plain({FirestoreClientDepKey: client}))
    hook = FirestoreStartupHook(project_id="my-project", database="(default)")

    await hook(ctx)

    client.initialize.assert_awaited_once_with(
        project_id="my-project",
        database="(default)",
    )


@pytest.mark.asyncio
async def test_firestore_shutdown_hook_closes_client() -> None:
    client = Mock(spec=FirestoreClient)
    client.close = AsyncMock(return_value=None)
    ctx = context_from_deps(Deps.plain({FirestoreClientDepKey: client}))
    hook = FirestoreShutdownHook()

    await hook(ctx)

    client.close.assert_awaited_once()


def test_firestore_lifecycle_step_builds_hooks() -> None:
    step = firestore_lifecycle_step(project_id="my-project")

    assert step.id == "firestore_lifecycle"
    assert isinstance(step.startup, FirestoreStartupHook)
    assert isinstance(step.shutdown, FirestoreShutdownHook)


class _MockRoutedFirestore:
    def __init__(self) -> None:
        self.startup_calls = 0
        self.close_calls = 0

    async def startup(self) -> None:
        self.startup_calls += 1

    async def close(self) -> None:
        self.close_calls += 1


@pytest.mark.asyncio
async def test_routed_firestore_lifecycle_step_invokes_client() -> None:
    client = _MockRoutedFirestore()
    ctx = context_from_deps(Deps.plain({FirestoreClientDepKey: client}))
    plan = LifecyclePlan.from_steps(routed_firestore_lifecycle_step(client=client))
    frozen = plan.freeze()

    await frozen.startup(ctx)
    await frozen.shutdown(ctx)

    assert client.startup_calls == 1
    assert client.close_calls == 1
