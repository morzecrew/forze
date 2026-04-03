from unittest.mock import Mock

from pydantic import BaseModel

from forze.application.contracts.queue import (
    QueueReadDepKey,
    QueueSpec,
    QueueWriteDepKey,
)
from forze.application.execution import Deps, ExecutionContext
from forze_rabbitmq.adapters import RabbitMQQueueAdapter
from forze_rabbitmq.execution.deps import RabbitMQClientDepKey, RabbitMQDepsModule
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
    context = ExecutionContext(deps=deps)
    spec = QueueSpec(name="events", model=_QueuePayload)

    reader = ConfigurableRabbitMQQueueRead(
        config={"namespace": "events", "tenant_aware": False},
    )
    queue = reader(context, spec)

    assert isinstance(queue, RabbitMQQueueAdapter)
    assert queue.client is rabbitmq_mock
    assert queue.codec.model is _QueuePayload
    assert queue.namespace == "events"

    writer = ConfigurableRabbitMQQueueWrite(
        config={"namespace": "events", "tenant_aware": False},
    )
    queue_w = writer(context, spec)
    assert isinstance(queue_w, RabbitMQQueueAdapter)


def test_rabbitmq_deps_module_registers_expected_keys() -> None:
    client = Mock(spec=RabbitMQClient)
    module = RabbitMQDepsModule(
        client=client,
        queue_readers={"q": {"namespace": "ns"}},
        queue_writers={"q": {"namespace": "ns"}},
    )

    deps = module()

    assert isinstance(deps, Deps)
    assert deps.exists(RabbitMQClientDepKey)
    assert deps.exists(QueueReadDepKey, route="q")
    assert deps.exists(QueueWriteDepKey, route="q")
