"""ClickHouse dependency module for the application kernel."""

from typing import final

import attrs

from forze.application.contracts.analytics import (
    AnalyticsIngestDepKey,
    AnalyticsQueryDepKey,
)
from forze.application.contracts.tenancy import (
    TenancyRouteGroup,
    TenantIsolationMode,
    validate_module_tenancy,
)
from forze.application.execution import Deps, DepsModule
from forze.application.execution.deps.builders import merge_deps, routed_from_mapping
from forze.base.primitives import MappingConverter, StrKeyMapping

from ...kernel.client import ClickHouseClientPort, RoutedClickHouseClient
from .configs import ClickHouseAnalyticsConfig
from .factories import ConfigurableClickHouseAnalytics
from .keys import ClickHouseClientDepKey

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ClickHouseDepsModule(DepsModule):
    """Dependency module that registers ClickHouse client and analytics adapters."""

    client: ClickHouseClientPort
    """Pre-constructed ClickHouse client (initialized via :func:`clickhouse_lifecycle_step`)."""

    analytics: StrKeyMapping[ClickHouseAnalyticsConfig] | None = attrs.field(
        default=None,
        converter=MappingConverter.to_str_key_frozen,  # type: ignore[misc]
    )
    """Mapping from analytics route names to ClickHouse configuration."""

    required_tenant_isolation: TenantIsolationMode | None = attrs.field(default=None)
    """Declared minimum tenant isolation (``None`` = no floor).

    Set ``"database"`` to require a routed (per-tenant) client — wiring fails closed if the
    client is shared, since row-level binding alone cannot isolate untrusted callers.
    """

    # ....................... #

    def __attrs_post_init__(self) -> None:
        validate_module_tenancy(
            integration="ClickHouse",
            client_is_routed=isinstance(self.client, RoutedClickHouseClient),
            groups=[
                TenancyRouteGroup(
                    kind="analytics",
                    configs=self.analytics,
                    tenant_aware=lambda cfg: cfg.tenant_aware,
                    namespace_resolver=lambda cfg: cfg.query_database,
                )
            ],
            required_isolation=self.required_tenant_isolation,
            validation_failed_code="clickhouse_analytics_tenancy_validation_failed",
        )

    # ....................... #

    def __call__(self) -> Deps:
        return merge_deps(
            routed_from_mapping(
                self.analytics,
                bindings=[
                    (AnalyticsQueryDepKey, ConfigurableClickHouseAnalytics),
                    (AnalyticsIngestDepKey, ConfigurableClickHouseAnalytics),
                ],
            ),
            plain={ClickHouseClientDepKey: self.client},
        )
