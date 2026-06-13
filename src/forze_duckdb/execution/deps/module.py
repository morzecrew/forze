"""DuckDB dependency module for the application kernel."""

from typing import final

import attrs

from forze.application.contracts.analytics import AnalyticsQueryDepKey
from forze.application.contracts.tenancy import (
    TenancyRouteSpec,
    TenantIsolationMode,
    validate_routed_client_tenancy_wiring,
)
from forze.application.execution import Deps, DepsModule
from forze.application.execution.deps.builders import merge_deps, routed_from_mapping
from forze.base.primitives import MappingConverter, StrKeyMapping

from ...kernel.client import DuckDbClientPort
from .configs import DuckDbAnalyticsConfig
from .factories import ConfigurableDuckDbAnalytics
from .keys import DuckDbClientDepKey

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class DuckDbDepsModule(DepsModule):
    """Dependency module that registers the DuckDB client and analytics query adapters.

    DuckDB is query-only, so only :data:`AnalyticsQueryDepKey` is bound (no ingest).
    """

    client: DuckDbClientPort
    """Pre-constructed DuckDB client (initialized via :func:`duckdb_lifecycle_step`)."""

    analytics: StrKeyMapping[DuckDbAnalyticsConfig] | None = attrs.field(
        default=None,
        converter=MappingConverter.to_str_key_frozen,  # type: ignore[misc]
    )
    """Mapping from analytics route names to DuckDB configuration."""

    required_tenant_isolation: TenantIsolationMode | None = attrs.field(default=None)
    """Declared minimum tenant isolation (``None`` = no floor).

    DuckDB is in-process with no per-tenant client routing, so it can never derive
    ``"database"`` isolation — declaring that floor fails closed by design (use row-level
    ``tenant_aware`` queries, or a networked backend with a routed client).
    """

    # ....................... #

    def __attrs_post_init__(self) -> None:
        routes = [
            TenancyRouteSpec(name=str(name), tenant_aware=cfg.tenant_aware, kind="analytics")
            for name, cfg in (self.analytics or {}).items()
        ]
        validate_routed_client_tenancy_wiring(
            integration="DuckDB",
            client_is_routed=False,
            partition_key_set=True,
            routes=routes,
            partition_key_detail="",
            validation_failed_code="duckdb_analytics_tenancy_validation_failed",
            required_isolation=self.required_tenant_isolation,
        )

    # ....................... #

    def __call__(self) -> Deps:
        return merge_deps(
            routed_from_mapping(
                self.analytics,
                bindings=[
                    (AnalyticsQueryDepKey, ConfigurableDuckDbAnalytics),
                ],
            ),
            plain={DuckDbClientDepKey: self.client},
        )
