from unittest.mock import Mock

from pydantic import BaseModel

from forze.application.contracts.queue import (
    QueueReadDepKey,
    QueueSpec,
    QueueWriteDepKey,
)
from forze.application.execution import Deps, ExecutionContext
from forze_sqs.adapters import SQSQueueAdapter
from forze_sqs.execution.deps import SQSClientDepKey, SQSDepsModule
from forze_sqs.execution.deps.deps import sqs_queue
from forze_sqs.kernel.platform import SQSClient


class _QueuePayload(BaseModel):
    value: str


def test_sqs_queue_builds_adapter() -> None:
    sqs_mock = Mock(spec=SQSClient)
    deps = Deps(deps={SQSClientDepKey: sqs_mock})
    context = ExecutionContext(deps=deps)
    spec = QueueSpec(namespace="events", model=_QueuePayload)

    queue = sqs_queue(context, spec)

    assert isinstance(queue, SQSQueueAdapter)
    assert queue.client is sqs_mock
    assert queue.codec.model is _QueuePayload
    assert queue.namespace == "events"


def test_sqs_deps_module_registers_expected_keys() -> None:
    client = Mock(spec=SQSClient)
    module = SQSDepsModule(client=client)

    deps = module()

    assert isinstance(deps, Deps)
    assert deps.exists(SQSClientDepKey)
    assert deps.exists(QueueReadDepKey)
    assert deps.exists(QueueWriteDepKey)
