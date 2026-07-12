"""Pytest configuration for forze_kafka integration tests (real brokers via testcontainers).

The whole suite runs against an engine matrix — Apache Kafka and Redpanda, two
independent implementations of the Kafka protocol — so nothing here quietly
specializes to one broker's behavior (admin-API response shapes, first-subscribe
and rebalance timing, ``offsets_for_times``). That is the same differential move
as mock ≡ real conformance, one level up: a divergence between engines is a
finding to fix in the adapter or declare in capabilities, never to special-case
per engine in ``src/``.
"""

from collections.abc import AsyncIterator, Iterator

import pytest
import pytest_asyncio
from docker import from_env
from docker.errors import DockerException
from testcontainers.kafka import KafkaContainer, RedpandaContainer

pytest.importorskip("aiokafka")

from forze.base.serialization import PydanticModelCodec
from forze_kafka.adapters import (
    KafkaCommitStreamGroupAdapter,
    KafkaCommitStreamGroupAdminAdapter,
    KafkaStreamCodec,
    KafkaStreamCommandAdapter,
)
from forze_kafka.kernel.client import KafkaClient, KafkaConfig

from _kafka_models import Payload

# ----------------------- #

__all__ = ["Payload"]


def _ensure_docker_available() -> None:
    client = None

    try:
        client = from_env()
        client.ping()
    except DockerException as exc:
        pytest.skip(f"Docker is required for Kafka integration tests: {exc}")
    finally:
        if client is not None:
            client.close()


# ....................... #


_REDPANDA_IMAGE = "docker.redpanda.com/redpandadata/redpanda:v26.1.12"


@pytest.fixture(scope="session", params=["kafka", "redpanda"])
def kafka_container(
    request: pytest.FixtureRequest,
) -> Iterator[KafkaContainer | RedpandaContainer]:
    """One broker per engine; every test in the suite runs against both."""

    _ensure_docker_available()

    broker: KafkaContainer | RedpandaContainer = (
        KafkaContainer()
        if request.param == "kafka"
        else RedpandaContainer(_REDPANDA_IMAGE)
    )

    with broker:
        yield broker


# ....................... #


def _codec() -> KafkaStreamCodec[Payload]:
    return KafkaStreamCodec(payload_codec=PydanticModelCodec(model_type=Payload))


# ....................... #


@pytest_asyncio.fixture(scope="function")
async def kafka_client(kafka_container: KafkaContainer) -> AsyncIterator[KafkaClient]:
    client = KafkaClient()
    await client.initialize(
        kafka_container.get_bootstrap_server(),
        config=KafkaConfig(auto_offset_reset="earliest"),
    )

    yield client

    await client.close()


# ....................... #


@pytest_asyncio.fixture(scope="function")
async def producer(
    kafka_client: KafkaClient,
) -> KafkaStreamCommandAdapter[Payload]:
    return KafkaStreamCommandAdapter(
        client=kafka_client,
        codec=_codec(),
        namespace="",
        tenant_aware=False,
        tenant_provider=lambda: None,
    )


@pytest_asyncio.fixture(scope="function")
async def consumer(
    kafka_client: KafkaClient,
) -> KafkaCommitStreamGroupAdapter[Payload]:
    return KafkaCommitStreamGroupAdapter(
        client=kafka_client,
        codec=_codec(),
        namespace="",
        tenant_aware=False,
        tenant_provider=lambda: None,
        auto_offset_reset="earliest",
    )


@pytest_asyncio.fixture(scope="function")
async def admin(
    kafka_client: KafkaClient,
) -> KafkaCommitStreamGroupAdminAdapter:
    return KafkaCommitStreamGroupAdminAdapter(
        client=kafka_client,
        namespace="",
        tenant_aware=False,
        tenant_provider=lambda: None,
    )
