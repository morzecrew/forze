from unittest.mock import AsyncMock, Mock

import pytest

from forze.application.execution import Deps, ExecutionContext
from forze_sqs.execution.deps import SQSClientDepKey
from forze_sqs.execution.lifecycle import (
    SQSShutdownHook,
    SQSStartupHook,
    sqs_lifecycle_step,
)
from forze_sqs.kernel.platform import SQSClient, SQSConfig


@pytest.mark.asyncio
async def test_sqs_startup_hook_initializes_client() -> None:
    client = Mock(spec=SQSClient)
    client.initialize = AsyncMock(return_value=None)
    ctx = ExecutionContext(deps=Deps.plain({SQSClientDepKey: client}))
    config = SQSConfig(connect_timeout=10)
    hook = SQSStartupHook(
        endpoint="http://localhost:4566",
        region_name="us-east-1",
        access_key_id="test",
        secret_access_key="test",
        config=config,
    )

    await hook(ctx)

    client.initialize.assert_awaited_once_with(
        endpoint="http://localhost:4566",
        region_name="us-east-1",
        access_key_id="test",
        secret_access_key="test",
        config=config,
    )


@pytest.mark.asyncio
async def test_sqs_shutdown_hook_closes_client() -> None:
    client = Mock(spec=SQSClient)
    client.close = AsyncMock(return_value=None)
    ctx = ExecutionContext(deps=Deps.plain({SQSClientDepKey: client}))
    hook = SQSShutdownHook()

    await hook(ctx)

    client.close.assert_awaited_once()


def test_sqs_lifecycle_step_builds_hooks() -> None:
    config = SQSConfig(connect_timeout=10)
    step = sqs_lifecycle_step(
        endpoint="http://localhost:4566",
        region_name="us-east-1",
        access_key_id="test",
        secret_access_key="test",
        config=config,
    )

    assert step.name == "sqs_lifecycle"
    assert isinstance(step.startup, SQSStartupHook)
    assert isinstance(step.shutdown, SQSShutdownHook)
