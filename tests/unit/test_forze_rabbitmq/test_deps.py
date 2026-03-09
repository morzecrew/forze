from unittest.mock import Mock

from pydantic import BaseModel

from forze.application.execution import Deps, ExecutionContext
from forze.application.contracts.queue import QueueReadDepKey, QueueSpec, QueueWriteDepKey
from forze_rabbitmq.adapters import RabbitMQQueueAdapter
from forze_rabbitmq.execution.deps import RabbitMQClientDepKey, RabbitMQDepsModule
from forze_rabbitmq.execution.deps.deps import rabbitmq_queue
from forze_rabbitmq.kernel.platform import RabbitMQClient


class _QueuePayload(BaseModel):
    value: str


def test_rabbitmq_queue_builds_adapter() -> None:
    rabbitmq_mock = Mock(spec=RabbitMQClient)
    deps = Deps(deps={RabbitMQClientDepKey: rabbitmq_mock})
    context = ExecutionContext(deps=deps)
    spec = QueueSpec(namespace="events", model=_QueuePayload)

    queue = rabbitmq_queue(context, spec)

    assert isinstance(queue, RabbitMQQueueAdapter)
    assert queue.client is rabbitmq_mock
    assert queue.codec.model is _QueuePayload
    assert queue.namespace == "events"


def test_rabbitmq_deps_module_registers_expected_keys() -> None:
    client = Mock(spec=RabbitMQClient)
    module = RabbitMQDepsModule(client=client)

    deps = module()

    assert isinstance(deps, Deps)
    assert deps.exists(RabbitMQClientDepKey)
    assert deps.exists(QueueReadDepKey)
    assert deps.exists(QueueWriteDepKey)
