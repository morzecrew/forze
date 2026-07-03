"""Kafka dependency module for the application kernel."""

from typing import final

import attrs

from forze.application.contracts.deps import (
    Deps,
    DepsModule,
    merge_deps,
    routed_from_mapping,
)
from forze.application.contracts.stream import (
    CommitStreamGroupAdminDepKey,
    CommitStreamGroupQueryDepKey,
    StreamCommandDepKey,
)
from forze.application.contracts.tenancy import (
    TenancyRouteGroup,
    TenantIsolationMode,
    validate_module_tenancy,
)
from forze.base.primitives import MappingConverter, StrKeyMapping

from ...kernel.client import KafkaClientPort, RoutedKafkaClient
from .configs import KafkaCommitStreamGroupConfig, KafkaStreamConfig
from .factories import (
    ConfigurableKafkaAdmin,
    ConfigurableKafkaConsume,
    ConfigurableKafkaProduce,
)
from .keys import KafkaClientDepKey

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class KafkaDepsModule(DepsModule):
    """Dependency module that registers a Kafka client and offset-log ports."""

    client: KafkaClientPort
    """Pre-constructed Kafka client (single-bootstrap or routed, not connected until lifecycle)."""

    streams: StrKeyMapping[KafkaStreamConfig] | None = attrs.field(
        default=None,
        converter=MappingConverter.to_str_key_frozen,  # type: ignore[misc]
    )
    """Mapping from produce route names to their Kafka configs.

    Registered under ``StreamCommandDepKey`` (append, encryption-wrapped per
    ``StreamSpec.encryption``)."""

    commit_groups: StrKeyMapping[KafkaCommitStreamGroupConfig] | None = attrs.field(
        default=None,
        converter=MappingConverter.to_str_key_frozen,  # type: ignore[misc]
    )
    """Mapping from offset-log consumer-group route names to their Kafka configs.

    Registered under ``CommitStreamGroupQueryDepKey`` (read/commit) and
    ``CommitStreamGroupAdminDepKey`` (topic/group/replay/lag)."""

    required_tenant_isolation: TenantIsolationMode | None = attrs.field(default=None)
    """Declared minimum tenant isolation (``None`` = no floor).

    Kafka spans ``namespace`` (per-tenant topic name via ``namespace``) and
    ``dedicated`` (a routed per-tenant client). It has no server-side row filter,
    so the ``tagged`` tier is not offered."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        validate_module_tenancy(
            integration="Kafka",
            client_is_routed=isinstance(self.client, RoutedKafkaClient),
            groups=[
                TenancyRouteGroup(
                    kind="stream",
                    configs=self.streams,
                    tenant_aware=lambda cfg: cfg.tenant_aware,
                    namespace_resolver=lambda cfg: cfg.namespace,
                ),
                TenancyRouteGroup(
                    kind="stream_group",
                    configs=self.commit_groups,
                    tenant_aware=lambda cfg: cfg.tenant_aware,
                    namespace_resolver=lambda cfg: cfg.namespace,
                ),
            ],
            required_isolation=self.required_tenant_isolation,
            max_supported_isolation="dedicated",
            validation_failed_code="kafka_tenancy_validation_failed",
        )

    # ....................... #

    def __call__(self) -> Deps:
        """Build a dependency container with Kafka-backed offset-log ports."""

        return merge_deps(
            routed_from_mapping(
                self.streams,
                bindings=[(StreamCommandDepKey, ConfigurableKafkaProduce)],
            ),
            routed_from_mapping(
                self.commit_groups,
                bindings=[
                    (CommitStreamGroupQueryDepKey, ConfigurableKafkaConsume),
                    (CommitStreamGroupAdminDepKey, ConfigurableKafkaAdmin),
                ],
            ),
            plain={KafkaClientDepKey: self.client},
        )
