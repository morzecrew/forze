"""Kafka wiring: config validation, factories, module registration, lifecycle, relation."""

from datetime import timedelta
from unittest.mock import Mock

import pytest

from forze.application.contracts.stream import StreamSpec
from forze.application.execution import Deps
from forze.base.exceptions import CoreException
from forze.base.serialization import PydanticModelCodec
from forze_kafka import (
    KafkaClient,
    KafkaClientDepKey,
    KafkaCommitStreamGroupConfig,
    KafkaConfig,
    KafkaDepsModule,
    KafkaStreamConfig,
    kafka_lifecycle_step,
    resolve_kafka_topic,
)
from forze_kafka.adapters import (
    KafkaCommitStreamGroupAdapter,
    KafkaCommitStreamGroupAdminAdapter,
    KafkaStreamCommandAdapter,
)
from forze_kafka.execution.deps import (
    ConfigurableKafkaAdmin,
    ConfigurableKafkaConsume,
    ConfigurableKafkaProduce,
)

from tests.support.execution_context import context_from_deps

from _kafka_fakes import Msg

# ----------------------- #


def _spec() -> StreamSpec[Msg]:
    return StreamSpec(name="events", codec=PydanticModelCodec(model_type=Msg))


# ---- config -------------------------------------------------------------- #


def test_config_defaults() -> None:
    config = KafkaConfig()

    assert config.security_protocol == "PLAINTEXT"
    assert config.enable_idempotence is True
    assert config.auto_offset_reset == "latest"


def test_config_rejects_nonpositive_timeout() -> None:
    with pytest.raises(CoreException):
        KafkaConfig(request_timeout=timedelta(seconds=0))


def test_config_sasl_requires_mechanism() -> None:
    with pytest.raises(CoreException):
        KafkaConfig(security_protocol="SASL_SSL")


def test_route_config_defaults() -> None:
    assert KafkaStreamConfig().namespace == ""
    group = KafkaCommitStreamGroupConfig()
    assert group.namespace == ""
    assert group.auto_offset_reset is None


# ---- factories ----------------------------------------------------------- #


def test_produce_factory_builds_adapter() -> None:
    client = Mock(spec=KafkaClient)
    ctx = context_from_deps(Deps.plain({KafkaClientDepKey: client}))

    produce = ConfigurableKafkaProduce(config=KafkaStreamConfig(namespace="events"))
    adapter = produce(ctx, _spec())

    assert isinstance(adapter, KafkaStreamCommandAdapter)
    assert adapter.client is client
    assert adapter.namespace == "events"


def test_consume_factory_builds_adapter() -> None:
    client = Mock(spec=KafkaClient)
    ctx = context_from_deps(Deps.plain({KafkaClientDepKey: client}))

    consume = ConfigurableKafkaConsume(
        config=KafkaCommitStreamGroupConfig(auto_offset_reset="earliest")
    )
    adapter = consume(ctx, _spec())

    assert isinstance(adapter, KafkaCommitStreamGroupAdapter)
    assert adapter.auto_offset_reset == "earliest"


def test_admin_factory_builds_adapter() -> None:
    client = Mock(spec=KafkaClient)
    ctx = context_from_deps(Deps.plain({KafkaClientDepKey: client}))

    admin = ConfigurableKafkaAdmin(config=KafkaCommitStreamGroupConfig())
    adapter = admin(ctx, _spec())

    assert isinstance(adapter, KafkaCommitStreamGroupAdminAdapter)


def test_factory_rejects_wrong_config_type() -> None:
    with pytest.raises(TypeError, match="KafkaStreamConfig"):
        ConfigurableKafkaProduce(config={"namespace": "x"})  # type: ignore[arg-type]


# ---- module -------------------------------------------------------------- #


def test_module_registers_client_key() -> None:
    module = KafkaDepsModule(
        client=Mock(spec=KafkaClient),
        streams={"events": KafkaStreamConfig()},
        commit_groups={"events": KafkaCommitStreamGroupConfig()},
    )

    deps = module()

    assert isinstance(deps, Deps)
    assert deps.exists(KafkaClientDepKey)


def test_module_empty_routes() -> None:
    module = KafkaDepsModule(client=Mock(spec=KafkaClient))

    assert isinstance(module(), Deps)


# ---- lifecycle ----------------------------------------------------------- #


def test_lifecycle_step_built() -> None:
    step = kafka_lifecycle_step(bootstrap_servers="localhost:9092")

    assert step.id == "kafka_lifecycle"


# ---- relation ------------------------------------------------------------ #


async def test_resolve_topic_without_namespace() -> None:
    assert await resolve_kafka_topic("", None, "events") == "events"


async def test_resolve_topic_with_namespace() -> None:
    assert await resolve_kafka_topic("ns", None, "events") == "ns.events"
