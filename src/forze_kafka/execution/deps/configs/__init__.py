"""Kafka execution configs."""

import attrs

from forze.application.contracts.resolution import (
    NamedResourceSpec,
    coerce_named_resource_spec,
)
from forze.application.contracts.tenancy import TenantAwareIntegrationConfig

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class KafkaStreamConfig(TenantAwareIntegrationConfig):
    """Configuration for a Kafka produce route (``StreamSpec`` → ``StreamCommandPort``).

    ``namespace`` is the offset-log ``namespace`` tenancy tier: a non-empty value
    prefixes the topic per tenant (``"{namespace}.{topic}"``). Leave it empty for
    single-tenant or ``dedicated`` (routed-client) deployments.
    """

    namespace: NamedResourceSpec = attrs.field(
        default="",
        converter=coerce_named_resource_spec,
    )
    """Per-tenant topic namespace (empty = topic name used as-is)."""


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class KafkaCommitStreamGroupConfig(TenantAwareIntegrationConfig):
    """Configuration for a Kafka consume + admin route (offset-log consumer group)."""

    namespace: NamedResourceSpec = attrs.field(
        default="",
        converter=coerce_named_resource_spec,
    )
    """Per-tenant topic namespace (empty = topic name used as-is)."""

    auto_offset_reset: str | None = None
    """First-consume position for a fresh group (``earliest`` / ``latest``); ``None``
    inherits the client default."""

    max_poll_records: int | None = None
    """Cap on records per consumer fetch; ``None`` inherits the client default."""
