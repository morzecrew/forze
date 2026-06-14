"""RabbitMQ dependency module for the application kernel."""

from typing import final

import attrs

from forze.application.contracts.queue import QueueCommandDepKey, QueueQueryDepKey
from forze.application.contracts.tenancy import (
    TenancyRouteGroup,
    TenantIsolationMode,
    validate_module_tenancy,
    warn_integration_routes,
)
from forze.application.execution import Deps, DepsModule
from forze.application.execution.deps.builders import merge_deps, routed_from_mapping
from forze.base.primitives import MappingConverter, StrKeyMapping

from ...kernel.client import RabbitMQClientPort, RoutedRabbitMQClient
from ._warnings import RABBITMQ_QUEUE_READER_WARNING, RABBITMQ_QUEUE_WRITER_WARNING
from .configs import RabbitMQQueueConfig
from .factories import ConfigurableRabbitMQQueueRead, ConfigurableRabbitMQQueueWrite
from .keys import RabbitMQClientDepKey

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class RabbitMQDepsModule(DepsModule):
    """Dependency module that registers RabbitMQ client and queue ports."""

    client: RabbitMQClientPort
    """Pre-constructed RabbitMQ client (single-DSN or routed, not connected until lifecycle)."""

    queue_readers: StrKeyMapping[RabbitMQQueueConfig] | None = attrs.field(
        default=None,
        converter=MappingConverter.to_str_key_frozen,  # type: ignore[misc]
    )
    """Mapping from queue names to their RabbitMQ-specific configurations."""

    queue_writers: StrKeyMapping[RabbitMQQueueConfig] | None = attrs.field(
        default=None,
        converter=MappingConverter.to_str_key_frozen,  # type: ignore[misc]
    )
    """Mapping from queue names to their RabbitMQ-specific configurations."""

    required_tenant_isolation: TenantIsolationMode | None = attrs.field(default=None)
    """Declared minimum tenant isolation (``None`` = no floor).

    Queues span: ``row`` (per-tenant name prefix via ``tenant_aware``), ``schema`` (a
    per-tenant ``namespace`` resolver), ``database`` (a routed per-tenant client).
    """

    # ....................... #

    def __attrs_post_init__(self) -> None:
        warn_integration_routes(
            integration="RabbitMQ",
            routes=self.queue_readers,
            warning=RABBITMQ_QUEUE_READER_WARNING,
        )
        warn_integration_routes(
            integration="RabbitMQ",
            routes=self.queue_writers,
            warning=RABBITMQ_QUEUE_WRITER_WARNING,
        )
        validate_module_tenancy(
            integration="RabbitMQ",
            client_is_routed=isinstance(self.client, RoutedRabbitMQClient),
            groups=[
                TenancyRouteGroup(
                    kind="queue",
                    configs=self.queue_readers,
                    tenant_aware=lambda cfg: cfg.tenant_aware,
                    namespace_resolver=lambda cfg: cfg.namespace,
                ),
                TenancyRouteGroup(
                    kind="queue",
                    configs=self.queue_writers,
                    tenant_aware=lambda cfg: cfg.tenant_aware,
                    namespace_resolver=lambda cfg: cfg.namespace,
                ),
            ],
            required_isolation=self.required_tenant_isolation,
            validation_failed_code="rabbitmq_tenancy_validation_failed",
            max_supported_isolation="database",
        )

    # ....................... #

    def __call__(self) -> Deps:
        """Build a dependency container with RabbitMQ-backed ports."""

        return merge_deps(
            routed_from_mapping(
                self.queue_readers,
                bindings=[(QueueQueryDepKey, ConfigurableRabbitMQQueueRead)],
            ),
            routed_from_mapping(
                self.queue_writers,
                bindings=[(QueueCommandDepKey, ConfigurableRabbitMQQueueWrite)],
            ),
            plain={RabbitMQClientDepKey: self.client},
        )
