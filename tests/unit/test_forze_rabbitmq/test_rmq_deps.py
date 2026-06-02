from unittest.mock import Mock

import pytest
from pydantic import BaseModel

from forze.application.contracts.queue import (
    QueueCommandDepKey,
    QueueQueryDepKey,
    QueueSpec,
)
from tests.support.execution_context import context_from_deps, context_from_modules, frozen_deps_from_deps
from forze.application.execution import Deps, ExecutionContext
from forze.base.serialization import PydanticModelCodec
from forze_rabbitmq.adapters import RabbitMQQueueAdapter
from forze_rabbitmq.execution.deps import (
    ConfigurableRabbitMQQueueRead,
    ConfigurableRabbitMQQueueWrite,
    RabbitMQClientDepKey,
    RabbitMQDepsModule,
    RabbitMQQueueConfig,
)
from forze_rabbitmq.kernel.client import RabbitMQClient


def test_rejects_mapping_config() -> None:
    with pytest.raises(TypeError, match="RabbitMQQueueConfig"):
        ConfigurableRabbitMQQueueRead(config={"namespace": "q"})


class _QueuePayload(BaseModel):
    value: str


def test_rabbitmq_queue_factory_builds_adapter() -> None:
    rabbitmq_mock = Mock(spec=RabbitMQClient)
    deps = Deps.plain({RabbitMQClientDepKey: rabbitmq_mock})
    context = context_from_deps(deps)
    spec = QueueSpec(
        name="events", codec=PydanticModelCodec(model_type=_QueuePayload)
    )

    reader = ConfigurableRabbitMQQueueRead(
        config=RabbitMQQueueConfig(namespace="events", tenant_aware=False),
    )
    queue = reader(context, spec)

    assert isinstance(queue, RabbitMQQueueAdapter)
    assert queue.client is rabbitmq_mock
    assert queue.codec.payload_codec.model_type is _QueuePayload
    assert queue.namespace == "events"

    writer = ConfigurableRabbitMQQueueWrite(
        config=RabbitMQQueueConfig(namespace="events", tenant_aware=False),
    )
    queue_w = writer(context, spec)
    assert isinstance(queue_w, RabbitMQQueueAdapter)


def test_rabbitmq_deps_module_registers_expected_keys() -> None:
    client = Mock(spec=RabbitMQClient)
    module = RabbitMQDepsModule(
        client=client,
        queue_readers={"q": RabbitMQQueueConfig(namespace="ns")},
        queue_writers={"q": RabbitMQQueueConfig(namespace="ns")},
    )

    deps = module()

    assert isinstance(deps, Deps)
    assert deps.exists(RabbitMQClientDepKey)
    assert deps.exists(QueueQueryDepKey, route="q")
    assert deps.exists(QueueCommandDepKey, route="q")
