"""DuckDB dependency module for the application kernel."""

from typing import final

import attrs

from forze.application.contracts.analytics import AnalyticsQueryDepKey
from forze.application.contracts.deps import Deps, DepsModule, merge_deps, routed_from_mapping
from forze.application.contracts.dynamic_read import DynamicReadDepKey
from forze.application.contracts.tenancy import (
    TenancyRouteGroup,
    TenantIsolationMode,
    validate_module_tenancy,
)
from forze.base.exceptions import exc
from forze.base.primitives import MappingConverter, StrKeyMapping

from ...kernel.client import DuckDbClientPort, RoutedDuckDbClient
from .configs import DuckDbAnalyticsConfig, DuckDbDynamicReadConfig
from .factories import ConfigurableDuckDbAnalytics, ConfigurableDuckDbDynamicRead
from .keys import DuckDbClientDepKey

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class DuckDbDepsModule(DepsModule):
    """Dependency module that registers the DuckDB client, analytics and dynamic-read adapters.

    DuckDB is query-only: :data:`AnalyticsQueryDepKey` and :data:`DynamicReadDepKey` are bound,
    never an ingest key.
    """

    client: DuckDbClientPort
    """Pre-constructed DuckDB client: a :class:`DuckDbClient` initialized via
    :func:`duckdb_lifecycle_step`, or a :class:`RoutedDuckDbClient` started by
    ``routed_client_lifecycle_step``."""

    analytics: StrKeyMapping[DuckDbAnalyticsConfig] | None = attrs.field(
        default=None,
        converter=MappingConverter.to_str_key_frozen,  # type: ignore[misc]
    )
    """Mapping from analytics route names to DuckDB configuration."""

    dynamic_reads: StrKeyMapping[DuckDbDynamicReadConfig] | None = attrs.field(
        default=None,
        converter=MappingConverter.to_str_key_frozen,  # type: ignore[misc]
    )
    """Mapping from governed dynamic-read route names to DuckDB configuration.

    Wires only on a :class:`RoutedDuckDbClient` and only with trusted provenance: a dynamic
    route runs runtime-compiled text, whose floor is ``namespace``, and only a per-tenant
    instance reaches a tier above ``tagged`` here."""

    required_tenant_isolation: TenantIsolationMode | None = attrs.field(default=None)
    """Declared minimum tenant isolation (``None`` = no floor).

    A plain :class:`DuckDbClient` is one instance for every tenant, so it never derives above
    ``"tagged"``. A :class:`RoutedDuckDbClient` derives ``"dedicated"``.
    """

    # ....................... #

    def __attrs_post_init__(self) -> None:
        routed = isinstance(self.client, RoutedDuckDbClient)
        reads = self.dynamic_reads or {}
        untrusted = sorted(
            str(name) for name, cfg in reads.items() if cfg.provenance == "untrusted"
        )

        if untrusted:
            raise exc.configuration(
                "DuckDB dynamic reads take trusted statements only: neither the timeout nor "
                "read-only is enforced outside the process, and every tenant shares its CPU.",
                code="duckdb_dynamic_read_untrusted_refused",
                details={"routes": untrusted},
            )

        if isinstance(self.client, RoutedDuckDbClient) and reads:
            ceiling = min(cfg.statement_timeout for cfg in reads.values())

            if self.client.http_timeout > ceiling:
                raise exc.configuration(
                    "RoutedDuckDbClient.http_timeout exceeds the smallest dynamic-read "
                    "statement_timeout on the client.",
                    code="duckdb_http_timeout_above_route_timeout",
                    details={
                        "http_timeout": str(self.client.http_timeout),
                        "statement_timeout": str(ceiling),
                    },
                )

        validate_module_tenancy(
            integration="DuckDB",
            client_is_routed=routed,
            groups=[
                TenancyRouteGroup(
                    kind="analytics",
                    configs=self.analytics,
                    tenant_aware=lambda cfg: cfg.tenant_aware,
                ),
                TenancyRouteGroup(
                    kind="dynamic_read",
                    configs=self.dynamic_reads,
                    tenant_aware=lambda cfg: cfg.tenant_aware,
                    # The plane's definition, unconditionally, as on Postgres.
                    origin=lambda _cfg: "compiled",
                ),
            ],
            required_isolation=self.required_tenant_isolation,
            max_supported_isolation="dedicated" if routed else "tagged",
            partition_key_set=routed,
            partition_key_detail=(
                "One unnamed :memory: instance per tenant holds every cache (RFC 0078 §2.3)."
            ),
            validation_failed_code="duckdb_analytics_tenancy_validation_failed",
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
            routed_from_mapping(
                self.dynamic_reads,
                bindings=[
                    (DynamicReadDepKey, ConfigurableDuckDbDynamicRead),
                ],
            ),
            plain={DuckDbClientDepKey: self.client},
        )
