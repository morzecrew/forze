"""BigQuery dependency module for the application kernel."""

from typing import final

import attrs

from forze.application.contracts.analytics import (
    AnalyticsIngestDepKey,
    AnalyticsQueryDepKey,
)
from forze.application.contracts.resolution import is_static_named_resource
from forze.application.contracts.tenancy import (
    TenancyRouteSpec,
    TenantIsolationMode,
    validate_routed_client_tenancy_wiring,
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
        configs = list((self.analytics or {}).values())
        routes = [
            TenancyRouteSpec(name=str(name), tenant_aware=cfg.tenant_aware, kind="analytics")
            for name, cfg in (self.analytics or {}).items()
        ]
        has_namespace_routing = any(
            cfg.query_dataset is not None
            and not is_static_named_resource(cfg.query_dataset)
            for cfg in configs
        )
        validate_routed_client_tenancy_wiring(
            integration="BigQuery",
            client_is_routed=isinstance(self.client, RoutedBigQueryClient),
            partition_key_set=True,
            routes=routes,
            partition_key_detail="",
            validation_failed_code="bigquery_analytics_tenancy_validation_failed",
            required_isolation=self.required_tenant_isolation,
            has_namespace_routing=has_namespace_routing,
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
