"""DuckDB analytics and dynamic-read execution configs."""

from datetime import timedelta
from typing import TYPE_CHECKING, Any, get_args

import attrs

from forze.application.contracts.dynamic_read import DynamicReadProvenance
from forze.application.contracts.tenancy import TenantAwareIntegrationConfig
from forze.application.integrations.analytics import assert_tenant_param_referenced
from forze.base.exceptions import exc
from forze.base.primitives import MappingConverter, StrKeyMapping

if TYPE_CHECKING:
    from forze.application.contracts.analytics import AnalyticsSpec

# ----------------------- #

_PROVENANCES = frozenset(get_args(DynamicReadProvenance))

# ....................... #


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


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class DuckDbDynamicReadConfig(TenantAwareIntegrationConfig):
    """Physical DuckDB mapping for one governed dynamic-read route, on a routed lake client.

    The container is the tenant's own instance, so there is no schema or role to configure.
    The route runs only on a :class:`~forze_duckdb.RoutedDuckDbClient`, and only trusted
    statements: the deps module refuses anything else at wiring.
    """

    provenance: DynamicReadProvenance
    """Who authored the statements this route executes. Mandatory; ``"untrusted"`` is refused
    by :class:`~forze_duckdb.execution.deps.module.DuckDbDepsModule`, because neither the
    timeout nor the read-only guarantee is enforced outside the process."""

    namespace: str
    """The lake namespace issued as ``USE lake.<namespace>`` before every statement, so a
    plan's two-part ``gold.orders`` and a bare ``orders`` resolve in the tenant's warehouse.
    An Iceberg catalog has no default schema, so ``USE lake`` alone is refused by DuckDB."""

    statement_timeout: timedelta = attrs.field(default=timedelta(seconds=5))
    """Statement timeout, enforced by cursor interrupt. A per-call option may clamp it down,
    never up."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        # The annotation is not enforced at runtime: a misspelt tier is refused here by name,
        # not left to the module's trusted-only check. A string is checked first, so an
        # unhashable value is refused rather than raising TypeError.
        provenance: object = self.provenance

        if not (isinstance(provenance, str) and provenance in _PROVENANCES):
            raise exc.configuration(
                f"DuckDB dynamic-read provenance={provenance!r} is not one of "
                f"{', '.join(sorted(_PROVENANCES))}.",
                code="duckdb_dynamic_read_provenance_invalid",
            )

        if self.statement_timeout.total_seconds() <= 0:
            raise exc.configuration(
                "DuckDB dynamic-read statement_timeout must be positive.",
                code="dynamic_read_timeout_invalid",
                details={"statement_timeout": str(self.statement_timeout)},
            )

        if not self.namespace.strip():
            raise exc.configuration(
                "DuckDB dynamic-read namespace must be non-empty.",
                code="duckdb_dynamic_read_namespace_invalid",
            )
