from unittest.mock import Mock

import pytest
from pydantic import BaseModel

from forze.application.contracts.queue import (
    QueueCommandDepKey,
    QueueQueryDepKey,
    QueueSpec,
)
from forze.application.execution import Deps
from forze.base.serialization import PydanticModelCodec
from forze_sqs.adapters import SQSQueueAdapter
from forze_sqs.execution.deps import (
    ConfigurableSQSQueueRead,
    ConfigurableSQSQueueWrite,
    SQSClientDepKey,
    SQSDepsModule,
    SQSQueueConfig,
)
from forze_sqs.kernel.client import SQSClient
from tests.support.execution_context import (
    context_from_deps,
)


class _QueuePayload(BaseModel):
    value: str


def test_rejects_mapping_config() -> None:
    with pytest.raises(TypeError, match="SQSQueueConfig"):
        ConfigurableSQSQueueRead(config={"namespace": "q"})


def test_sqs_queue_factory_builds_adapter() -> None:
    sqs_mock = Mock(spec=SQSClient)
    deps = Deps.plain({SQSClientDepKey: sqs_mock})
    context = context_from_deps(deps)
    spec = QueueSpec(
        name="events",
        codec=PydanticModelCodec(model_type=_QueuePayload),
    )

    reader = ConfigurableSQSQueueRead(
        config=SQSQueueConfig(namespace="events", tenant_aware=False),
    )
    queue = reader(context, spec)

    assert isinstance(queue, SQSQueueAdapter)
    assert queue.client is sqs_mock
    assert queue.codec.payload_codec.model_type is _QueuePayload
    assert queue.namespace == "events"

    writer = ConfigurableSQSQueueWrite(
        config=SQSQueueConfig(namespace="events", tenant_aware=False),
    )
    assert isinstance(writer(context, spec), SQSQueueAdapter)


def test_sqs_deps_module_registers_expected_keys() -> None:
    client = Mock(spec=SQSClient)
    module = SQSDepsModule(
        client=client,
        queue_readers={"events": SQSQueueConfig(namespace="ns")},
        queue_writers={"events": SQSQueueConfig(namespace="ns")},
    )

    deps = module()

    assert isinstance(deps, Deps)
    assert deps.exists(SQSClientDepKey)
    assert deps.exists(QueueQueryDepKey, route="events")
    assert deps.exists(QueueCommandDepKey, route="events")
