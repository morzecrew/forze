"""BigQuery dependency module for the application kernel."""

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

from ...kernel.client import BigQueryClientPort, RoutedBigQueryClient
from .configs import BigQueryAnalyticsConfig
from .factories import ConfigurableBigQueryAnalytics
from .keys import BigQueryClientDepKey

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class BigQueryDepsModule(DepsModule):
    """Dependency module that registers BigQuery client and analytics adapters."""

    client: BigQueryClientPort
    """Pre-constructed BigQuery client (initialized via :func:`bigquery_lifecycle_step`)."""

    analytics: StrKeyMapping[BigQueryAnalyticsConfig] | None = attrs.field(
        converter=MappingConverter.to_str_key_frozen,  # type: ignore[misc]
        default=None,
    )
    """Mapping from analytics route names to BigQuery configuration."""

    required_tenant_isolation: TenantIsolationMode | None = attrs.field(default=None)
    """Declared minimum tenant isolation (``None`` = no floor).

    Set ``"database"`` to require a routed (per-tenant) client — wiring fails closed if the
    client is shared, since row-level binding alone cannot isolate untrusted callers.
    """

    # ....................... #

    def __attrs_post_init__(self) -> None:
        validate_module_tenancy(
            integration="BigQuery",
            client_is_routed=isinstance(self.client, RoutedBigQueryClient),
            groups=[
                TenancyRouteGroup(
                    kind="analytics",
                    configs=self.analytics,
                    tenant_aware=lambda cfg: cfg.tenant_aware,
                    namespace_resolver=lambda cfg: cfg.query_dataset,
                )
            ],
            required_isolation=self.required_tenant_isolation,
            validation_failed_code="bigquery_analytics_tenancy_validation_failed",
            max_supported_isolation="database",
        )

    # ....................... #

    def __call__(self) -> Deps:
        return merge_deps(
            routed_from_mapping(
                self.analytics,
                bindings=[
                    (AnalyticsQueryDepKey, ConfigurableBigQueryAnalytics),
                    (AnalyticsIngestDepKey, ConfigurableBigQueryAnalytics),
                ],
            ),
            plain={BigQueryClientDepKey: self.client},
        )
