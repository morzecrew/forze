from unittest.mock import AsyncMock, Mock

import pytest

from forze.application.execution import Deps, ExecutionContext
from forze_rabbitmq.execution.deps import RabbitMQClientDepKey
from forze_rabbitmq.execution.lifecycle import (
    RabbitMQShutdownHook,
    RabbitMQStartupHook,
    rabbitmq_lifecycle_step,
)
from forze_rabbitmq.kernel.platform import RabbitMQClient, RabbitMQConfig


@pytest.mark.asyncio
async def test_rabbitmq_startup_hook_initializes_client() -> None:
    client = Mock(spec=RabbitMQClient)
    client.initialize = AsyncMock(return_value=None)
    ctx = ExecutionContext(deps=Deps(deps={RabbitMQClientDepKey: client}))
    config = RabbitMQConfig(prefetch_count=10)
    hook = RabbitMQStartupHook(dsn="amqp://guest:guest@localhost/", config=config)

    await hook(ctx)

    client.initialize.assert_awaited_once_with(
        "amqp://guest:guest@localhost/",
        config=config,
    )


@pytest.mark.asyncio
async def test_rabbitmq_shutdown_hook_closes_client() -> None:
    client = Mock(spec=RabbitMQClient)
    client.close = AsyncMock(return_value=None)
    ctx = ExecutionContext(deps=Deps(deps={RabbitMQClientDepKey: client}))
    hook = RabbitMQShutdownHook()

    await hook(ctx)

    client.close.assert_awaited_once()


def test_rabbitmq_lifecycle_step_builds_hooks() -> None:
    config = RabbitMQConfig(prefetch_count=20)
    step = rabbitmq_lifecycle_step(dsn="amqp://guest:guest@localhost/", config=config)

    assert step.name == "rabbitmq_lifecycle"
    assert isinstance(step.startup, RabbitMQStartupHook)
    assert isinstance(step.shutdown, RabbitMQShutdownHook)
