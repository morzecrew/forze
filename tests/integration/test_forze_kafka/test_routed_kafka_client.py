"""Integration tests for :class:`~forze_kafka.kernel.client.RoutedKafkaClient`.

Drives the routed facade against a real broker: a per-tenant bootstrap string is
resolved from a tiny in-memory ``SecretsPort``, and a full produce → consume →
commit round-trip plus ``health`` / ``group_config`` exercises every delegating
method (``send`` / ``get_consumer`` / ``new_transient_consumer`` / ``admin``).
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import timedelta
from uuid import UUID, uuid4

import pytest

pytest.importorskip("aiokafka")

from forze.application.contracts.secrets import SecretRef
from forze.application.contracts.stream import StreamPosition
from forze.base.exceptions import CoreException, exc
from forze.base.serialization import PydanticModelCodec
from forze_kafka.adapters import (
    KafkaCommitStreamGroupAdapter,
    KafkaCommitStreamGroupAdminAdapter,
    KafkaStreamCodec,
    KafkaStreamCommandAdapter,
)
from forze_kafka.kernel.client import KafkaConfig, RoutedKafkaClient

from _kafka_models import Payload

# ----------------------- #


def _ref(tenant_id: UUID) -> SecretRef:
    return SecretRef(path=f"tenants/{tenant_id}/kafka")


class _MemSecrets:
    """In-memory ``SecretsPort``: a tenant path → its bootstrap-servers string."""

    def __init__(self, paths: dict[str, str]) -> None:
        self._paths = paths

    async def resolve_str(self, ref: SecretRef) -> str:
        try:
            return self._paths[ref.path]
        except KeyError as e:
            raise exc.not_found(
                f"No secret for {ref.path!r}", details={"ref": ref.path}
            ) from e

    async def exists(self, ref: SecretRef) -> bool:
        return ref.path in self._paths


def _tenant_holder() -> tuple[Callable[[], UUID | None], Callable[[UUID | None], None]]:
    slot: list[UUID | None] = [None]
    return (lambda: slot[0]), (lambda value: slot.__setitem__(0, value))


def _codec() -> KafkaStreamCodec[Payload]:
    return KafkaStreamCodec(payload_codec=PydanticModelCodec(model_type=Payload))


# ....................... #


async def test_routed_client_round_trip_and_delegation(
    kafka_container,  # noqa: ANN001 - session container fixture
) -> None:
    bootstrap = kafka_container.get_bootstrap_server()
    tenant = uuid4()
    get_tenant, set_tenant = _tenant_holder()

    client = RoutedKafkaClient(
        secrets=_MemSecrets({f"tenants/{tenant}/kafka": bootstrap}),  # type: ignore[arg-type]
        secret_ref_for_tenant=_ref,
        tenant_provider=get_tenant,
        connection_config=KafkaConfig(auto_offset_reset="earliest"),
    )
    await client.startup()
    set_tenant(tenant)

    try:
        # group_config falls back to the connection config before any client exists.
        assert client.group_config()["auto_offset_reset"] == "earliest"

        producer = KafkaStreamCommandAdapter(
            client=client,
            codec=_codec(),
            namespace="",
            tenant_aware=False,
            tenant_provider=lambda: None,
        )
        consumer = KafkaCommitStreamGroupAdapter(
            client=client,
            codec=_codec(),
            namespace="",
            tenant_aware=False,
            tenant_provider=lambda: None,
            auto_offset_reset="earliest",
        )
        admin = KafkaCommitStreamGroupAdminAdapter(
            client=client,
            namespace="",
            tenant_aware=False,
            tenant_provider=lambda: None,
        )

        topic = f"it-routed-{uuid4().hex[:8]}"
        group = f"g-{uuid4().hex[:8]}"

        # admin() delegation
        await admin.ensure_topic(topic, partitions=1)

        # send() delegation
        for i in range(3):
            message_id = await producer.append(topic, Payload(value=str(i)), key="k")
            assert message_id.startswith(f"{topic}:")

        # get_consumer() delegation + commit
        positions: list[StreamPosition] = []
        for _ in range(20):
            batch = await consumer.read(
                group, "m1", [topic], timeout=timedelta(seconds=1)
            )
            positions.extend(StreamPosition.from_message(m) for m in batch)
            if len(positions) >= 3:
                break
        assert len(positions) == 3
        await consumer.commit(group, positions)

        # admin() + new_transient_consumer() delegation (lag over a transient consumer)
        lags = await admin.lag(group, topic)
        assert lags and sum(lag.lag for lag in lags) == 0

        # health() + group_config() delegation once a client is live
        name, healthy = await client.health()
        assert name == "Kafka"
        assert healthy is True
        assert client.group_config()["auto_offset_reset"] == "earliest"
    finally:
        await client.close()


async def test_routed_client_requires_tenant(
    kafka_container,  # noqa: ANN001 - session container fixture
) -> None:
    bootstrap = kafka_container.get_bootstrap_server()
    tenant = uuid4()
    get_tenant, _set_tenant = _tenant_holder()  # tenant left unset

    client = RoutedKafkaClient(
        secrets=_MemSecrets({f"tenants/{tenant}/kafka": bootstrap}),  # type: ignore[arg-type]
        secret_ref_for_tenant=_ref,
        tenant_provider=get_tenant,
    )
    await client.startup()

    try:
        with pytest.raises(CoreException):
            await client.health()  # no bound tenant → routed access is refused
    finally:
        await client.close()
