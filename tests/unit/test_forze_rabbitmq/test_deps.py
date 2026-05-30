from unittest.mock import Mock

from pydantic import BaseModel

from forze.application.contracts.queue import (
    QueueCommandDepKey,
    QueueQueryDepKey,
    QueueSpec,
)
from tests.support.execution_context import context_from_deps, context_from_modules, frozen_deps_from_deps
from forze.application.execution import Deps, ExecutionContext
from forze.base.serialization import PydanticRecordMappingCodec
from forze_rabbitmq.adapters import RabbitMQQueueAdapter
from forze_rabbitmq.execution.deps import RabbitMQClientDepKey, RabbitMQDepsModule
from forze_rabbitmq.execution.deps.configs import RabbitMQQueueConfig
from forze_rabbitmq.execution.deps.deps import (
    ConfigurableRabbitMQQueueRead,
    ConfigurableRabbitMQQueueWrite,
)
from forze_rabbitmq.kernel.platform import RabbitMQClient


class _QueuePayload(BaseModel):
    value: str


def test_rabbitmq_queue_factory_builds_adapter() -> None:
    rabbitmq_mock = Mock(spec=RabbitMQClient)
    deps = Deps.plain({RabbitMQClientDepKey: rabbitmq_mock})
    context = context_from_deps(deps)
    spec = QueueSpec(
        name="events", codec=PydanticRecordMappingCodec(model_type=_QueuePayload)
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
