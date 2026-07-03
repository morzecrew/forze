"""Kafka client that resolves bootstrap servers per tenant via :class:`~forze.application.contracts.secrets.SecretsPort`."""

from typing import Callable, Mapping, Sequence, final
from uuid import UUID

import attrs
from aiokafka import AIOKafkaConsumer
from aiokafka.admin import AIOKafkaAdminClient
from aiokafka.structs import RecordMetadata

from forze.application.contracts.secrets import SecretRef, SecretsPort
from forze.application.contracts.tenancy.routed_client_base import (
    DsnRoutedTenantClientBase,
)

from .client import KafkaClient
from .port import KafkaClientPort
from .value_objects import KafkaConfig

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True)
class RoutedKafkaClient(DsnRoutedTenantClientBase[KafkaClient], KafkaClientPort):
    """Routes each call to a lazily created :class:`KafkaClient` for the current tenant.

    Bootstrap-server strings are resolved via :meth:`SecretsPort.resolve_str` and
    ``secret_ref_for_tenant`` — the ``dedicated`` tenancy tier for offset-log
    streams. Use
    :func:`~forze_kafka.execution.lifecycle.routed_kafka_lifecycle_step` after
    registering the same instance under :data:`KafkaClientDepKey`.
    """

    secrets: SecretsPort
    secret_ref_for_tenant: Callable[[UUID], SecretRef] | Mapping[UUID, SecretRef]
    tenant_provider: Callable[[], UUID | None]
    connection_config: KafkaConfig = attrs.field(factory=KafkaConfig)
    max_cached_tenants: int = 100
    dsn_backend: str = attrs.field(default="Kafka", init=False)
    tenant_required_message: str = attrs.field(
        default="Tenant ID is required for routed Kafka access",
        init=False,
    )

    async def initialize_client(self, tenant_id: UUID, creds: str) -> KafkaClient:
        client = KafkaClient()
        await client.initialize(creds, config=self.connection_config)

        return client

    # ....................... #

    async def health(self) -> tuple[str, bool]:
        inner = await self._get_client()
        return await inner.health()

    # ....................... #

    async def send(
        self,
        topic: str,
        value: bytes,
        *,
        key: bytes | None = None,
        headers: Sequence[tuple[str, bytes]] | None = None,
        timestamp_ms: int | None = None,
    ) -> RecordMetadata:
        inner = await self._get_client()
        return await inner.send(
            topic,
            value,
            key=key,
            headers=headers,
            timestamp_ms=timestamp_ms,
        )

    # ....................... #

    async def get_consumer(
        self,
        *,
        group: str,
        member: str,
        topics: Sequence[str],
        auto_offset_reset: str | None = None,
        max_poll_records: int | None = None,
    ) -> AIOKafkaConsumer:
        inner = await self._get_client()
        return await inner.get_consumer(
            group=group,
            member=member,
            topics=topics,
            auto_offset_reset=auto_offset_reset,
            max_poll_records=max_poll_records,
        )

    # ....................... #

    async def new_transient_consumer(
        self,
        *,
        group: str | None = None,
    ) -> AIOKafkaConsumer:
        inner = await self._get_client()
        return await inner.new_transient_consumer(group=group)

    # ....................... #

    async def admin(self) -> AIOKafkaAdminClient:
        inner = await self._get_client()
        return await inner.admin()

    # ....................... #

    def group_config(self) -> Mapping[str, object]:
        peeked = self._peek_client()

        if peeked is not None:
            return peeked.group_config()

        return {
            "auto_offset_reset": self.connection_config.auto_offset_reset,
            "max_poll_records": self.connection_config.max_poll_records,
        }
