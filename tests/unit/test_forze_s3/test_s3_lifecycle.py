"""Unit tests for :mod:`forze_s3.execution.lifecycle.pool`."""

from unittest.mock import AsyncMock, Mock

import pytest
from pydantic import SecretStr

from forze.application.execution import Deps, LifecyclePlan
from forze_s3.execution.deps import S3ClientDepKey
from forze_s3.execution.lifecycle import (
    S3ShutdownHook,
    S3StartupHook,
    routed_s3_lifecycle_step,
    s3_lifecycle_step,
)
from forze_s3.kernel.client import S3Client, S3Config
from tests.support.execution_context import context_from_deps


@pytest.mark.asyncio
async def test_s3_startup_hook_initializes_client() -> None:
    client = Mock(spec=S3Client)
    client.initialize = AsyncMock(return_value=None)
    ctx = context_from_deps(Deps.plain({S3ClientDepKey: client}))
    config = S3Config()
    hook = S3StartupHook(
        endpoint="http://localhost:9000",
        access_key_id="minio",
        secret_access_key="minio123",
        config=config,
    )

    await hook(ctx)

    client.initialize.assert_awaited_once_with(
        "http://localhost:9000",
        "minio",
        SecretStr("minio123"),
        config=config,
    )


@pytest.mark.asyncio
async def test_s3_shutdown_hook_closes_client() -> None:
    client = Mock(spec=S3Client)
    client.close = AsyncMock(return_value=None)
    ctx = context_from_deps(Deps.plain({S3ClientDepKey: client}))
    hook = S3ShutdownHook()

    await hook(ctx)

    client.close.assert_awaited_once()


def test_s3_lifecycle_step_builds_hooks() -> None:
    step = s3_lifecycle_step(
        endpoint="http://localhost:9000",
        access_key_id="minio",
        secret_access_key="minio123",
    )

    assert step.id == "s3_lifecycle"
    assert isinstance(step.startup, S3StartupHook)
    assert isinstance(step.shutdown, S3ShutdownHook)


class _MockRoutedS3:
    def __init__(self) -> None:
        self.startup_calls = 0
        self.close_calls = 0

    async def startup(self) -> None:
        self.startup_calls += 1

    async def close(self) -> None:
        self.close_calls += 1


@pytest.mark.asyncio
async def test_routed_s3_lifecycle_step_invokes_client() -> None:
    client = _MockRoutedS3()
    ctx = context_from_deps(Deps.plain({S3ClientDepKey: client}))
    plan = LifecyclePlan.from_steps(routed_s3_lifecycle_step(client=client))
    frozen = plan.freeze()

    await frozen.startup(ctx)
    await frozen.shutdown(ctx)

    assert client.startup_calls == 1
    assert client.close_calls == 1
