from unittest.mock import Mock

from pydantic import BaseModel

from forze.application.contracts.queue import (
    QueueCommandDepKey,
    QueueQueryDepKey,
    QueueSpec,
)
from forze.application.execution import Deps, ExecutionContext
from forze_sqs.adapters import SQSQueueAdapter
from forze_sqs.execution.deps import SQSClientDepKey, SQSDepsModule
from forze_sqs.execution.deps.deps import ConfigurableSQSQueueRead, ConfigurableSQSQueueWrite
from forze_sqs.kernel.platform import SQSClient


class _QueuePayload(BaseModel):
    value: str


def test_sqs_queue_factory_builds_adapter() -> None:
    sqs_mock = Mock(spec=SQSClient)
    deps = Deps.plain({SQSClientDepKey: sqs_mock})
    context = ExecutionContext(deps=deps)
    spec = QueueSpec(name="events", model=_QueuePayload)

    reader = ConfigurableSQSQueueRead(
        config={"namespace": "events", "tenant_aware": False},
    )
    queue = reader(context, spec)

    assert isinstance(queue, SQSQueueAdapter)
    assert queue.client is sqs_mock
    assert queue.codec.model is _QueuePayload
    assert queue.namespace == "events"

    writer = ConfigurableSQSQueueWrite(
        config={"namespace": "events", "tenant_aware": False},
    )
    assert isinstance(writer(context, spec), SQSQueueAdapter)


def test_sqs_deps_module_registers_expected_keys() -> None:
    client = Mock(spec=SQSClient)
    module = SQSDepsModule(
        client=client,
        queue_readers={"events": {"namespace": "ns"}},
        queue_writers={"events": {"namespace": "ns"}},
    )

    deps = module()

    assert isinstance(deps, Deps)
    assert deps.exists(SQSClientDepKey)
    assert deps.exists(QueueQueryDepKey, route="events")
    assert deps.exists(QueueCommandDepKey, route="events")
