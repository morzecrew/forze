"""Kafka topic namespace resolution."""

from uuid import UUID

from forze.application.contracts.resolution import (
    NamedResourceSpec,
    coerce_named_resource_spec,
    resolve_value,
)

__all__ = [
    "NamedResourceSpec",
    "coerce_named_resource_spec",
    "resolve_kafka_topic",
]


# ....................... #


async def resolve_kafka_topic(
    namespace: NamedResourceSpec,
    tenant_id: UUID | None,
    topic: str,
) -> str:
    """Resolve *topic* to its physical name under *namespace* for *tenant_id*.

    An empty namespace leaves the topic unchanged (the single-tenant / dedicated
    path); a non-empty namespace prefixes the topic (``"{namespace}.{topic}"``),
    which is how the offset-log ``namespace`` tenancy tier isolates per-tenant
    topics. ``dedicated`` tenancy routes at the client instead and keeps the
    topic name shared.
    """

    prefix = await resolve_value(namespace, tenant_id)

    return f"{prefix}.{topic}" if prefix else topic
