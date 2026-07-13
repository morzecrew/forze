"""Kafka producer adapter implementing :class:`StreamCommandPort`."""

from collections.abc import Mapping
from datetime import datetime
from typing import final
from uuid import UUID

import attrs

from forze.application.contracts.stream import StreamCommandPort
from forze.application.contracts.tenancy import TenantProviderPort

from ..kernel.client import KafkaClientPort
from ..kernel.relation import NamedResourceSpec, resolve_kafka_topic
from .codecs import KafkaStreamCodec

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class KafkaStreamCommandAdapter[M](StreamCommandPort[M]):
    """Appends to a Kafka topic via ``send_and_wait``, returning the position id.

    ``key`` becomes the native Kafka message key (default murmur2 partitioner →
    same key, same partition → per-key ordering), ``headers`` ride native record
    headers, and ``timestamp`` the native record timestamp. The returned id is the
    canonical ``"{topic}:{partition}:{offset}"`` from the produce metadata, so a
    consumed message's :class:`StreamPosition` round-trips it.
    """

    client: KafkaClientPort
    codec: KafkaStreamCodec[M]
    namespace: NamedResourceSpec
    tenant_aware: bool
    tenant_provider: TenantProviderPort

    # ....................... #

    def _tenant_id(self) -> UUID | None:
        if not self.tenant_aware:
            return None

        tenant = self.tenant_provider()

        return None if tenant is None else tenant.tenant_id

    # ....................... #

    async def append(
        self,
        stream: str,
        payload: M,
        *,
        type: str | None = None,
        key: str | None = None,
        timestamp: datetime | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> str:
        topic = await resolve_kafka_topic(self.namespace, self._tenant_id(), stream)

        metadata = await self.client.send(
            topic,
            self.codec.encode_value(payload),
            key=key.encode("utf-8") if key is not None else None,
            headers=self.codec.encode_headers(type=type, headers=headers),
            timestamp_ms=(int(timestamp.timestamp() * 1000) if timestamp is not None else None),
        )

        return f"{metadata.topic}:{metadata.partition}:{metadata.offset}"
