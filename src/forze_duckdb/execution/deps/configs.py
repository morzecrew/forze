"""DuckDB analytics execution configs."""

from typing import TYPE_CHECKING, Any

import attrs

from forze.application.contracts.tenancy import TenantAwareIntegrationConfig
from forze.application.integrations.analytics import assert_tenant_param_referenced
from forze.base.exceptions import exc
from forze.base.primitives import MappingConverter, StrKeyMapping

if TYPE_CHECKING:
    from forze.application.contracts.analytics import AnalyticsSpec

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class DuckDbQueryConfig:
    """SQL for one named analytics query."""

    sql: str
    """DuckDB SQL; bind params with ``$name`` matching the spec params model.

    Sources may be inlined (``... FROM read_parquet('s3://bucket/*.parquet')``) or
    reference a view registered at client startup (``sources`` / ``bootstrap_sql``).
    """

    skip_total: bool = False
    """When True, ``run_page`` skips the COUNT wrapper."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if not self.sql.strip():
            raise exc.configuration("Analytics query sql must be non-empty.")


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class DuckDbAnalyticsConfig(TenantAwareIntegrationConfig):
    """Physical DuckDB mapping for one :class:`~forze.application.contracts.analytics.AnalyticsSpec` route.

    When ``tenant_aware`` (inherited), the adapter binds the current tenant id as the
    ``$tenant`` query parameter and fails closed if no tenant is bound; every registered
    query SQL must reference that parameter (checked at wiring).
    """

    queries: StrKeyMapping[DuckDbQueryConfig] = attrs.field(
        converter=MappingConverter.to_str_key_frozen,  # type: ignore[misc]
    )
    """Named queries; keys must match ``AnalyticsSpec.queries``."""

    # ....................... #

    def validate_against_spec(self, spec: "AnalyticsSpec[Any, Any]") -> None:
        spec_keys = set(spec.queries.keys())
        config_keys = set(self.queries.keys())

        missing = spec_keys - config_keys

        if missing:
            raise exc.configuration(
                f"DuckDB analytics config for route {spec.name!r} is missing query keys: "
                f"{sorted(missing)!r}."
            )

        extra = config_keys - spec_keys

        if extra:
            raise exc.configuration(
                f"DuckDB analytics config for route {spec.name!r} has unknown query keys: "
                f"{sorted(extra)!r}."
            )

        if spec.ingest is not None:
            raise exc.configuration(
                f"DuckDB analytics config for route {spec.name!r} cannot serve "
                "AnalyticsSpec.ingest: the DuckDB integration is query-only."
            )

        if self.tenant_aware:
            assert_tenant_param_referenced(
                {str(k): v.sql for k, v in self.queries.items()},
                pattern=r"\$tenant\b",
                placeholder_hint="$tenant",
                integration="DuckDB",
                route=str(spec.name),
            )
