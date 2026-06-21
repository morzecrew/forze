"""BigQuery analytics execution configs."""

from typing import TYPE_CHECKING, Any

import attrs

from forze.application.contracts.analytics import IngestSpec, coerce_optional_ingest
from forze.application.contracts.resolution import (
    NamedResourceSpec,
    coerce_optional_named_resource_spec,
)
from forze.application.contracts.tenancy import TenantAwareIntegrationConfig
from forze.application.integrations.analytics import assert_tenant_param_referenced
from forze.base.exceptions import exc
from forze.base.primitives import MappingConverter, StrKeyMapping
from forze_bigquery.kernel.relation import RelationSpec

if TYPE_CHECKING:
    from forze.application.contracts.analytics import AnalyticsSpec

# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class BigQueryQueryConfig:
    """SQL and options for one named analytics query."""

    sql: str
    """Standard SQL template using ``@param`` names matching the spec params model."""

    maximum_bytes_billed: int | None = None
    """Per-query override for maximum bytes billed."""

    skip_total: bool = False
    """When True, ``run_page`` skips the COUNT wrapper."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if not self.sql.strip():
            raise exc.internal("Analytics query sql must be non-empty.")


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class BigQueryAnalyticsConfig(TenantAwareIntegrationConfig):
    """Physical BigQuery mapping for one :class:`~forze.application.contracts.analytics.AnalyticsSpec` route.

    When ``tenant_aware`` (inherited), the adapter binds the current tenant id as the
    ``@tenant`` query parameter and fails closed if no tenant is bound; every registered
    query SQL must reference that parameter (checked at wiring).
    """

    dataset: str
    """BigQuery dataset id (used for ingest; see :attr:`query_dataset` for per-tenant query)."""

    queries: StrKeyMapping[BigQueryQueryConfig] = attrs.field(
        converter=MappingConverter.to_str_key_frozen,  # type: ignore[misc]
    )
    """Named queries; keys must match ``AnalyticsSpec.queries``."""

    query_dataset: NamedResourceSpec | None = attrs.field(
        default=None,
        converter=coerce_optional_named_resource_spec,
    )
    """Per-tenant query dataset — a static name or ``(tenant_id) -> str`` resolver.

    When set, queries run with this as the BigQuery *default dataset*, so an unqualified
    table in the registered SQL resolves in the tenant's own dataset (dataset-per-tenant
    isolation on a shared project). ``None`` leaves the default dataset unset (the SQL must
    fully-qualify its tables — the prior behavior).
    """

    ingest: IngestSpec | None = attrs.field(
        default=None,
        converter=coerce_optional_ingest,
    )
    """Append-only ingest target ``(dataset, table)`` or tenant resolver (relation-level isolation)."""

    insert_id_field: str | None = None
    """Optional row field used as streaming ``insertId``."""

    max_append_rows: int = 10_000
    """Maximum rows per ``append`` call."""

    # ....................... #

    def resolved_ingest_relation(self) -> RelationSpec | None:
        """Effective ingest relation, or ``None`` when no ingest target is configured."""

        return self.ingest.relation if self.ingest is not None else None

    # ....................... #

    def validate_against_spec(self, spec: "AnalyticsSpec[Any, Any]") -> None:
        spec_keys = set(spec.queries.keys())
        config_keys = set(self.queries.keys())

        if missing := spec_keys - config_keys:
            raise exc.configuration(
                f"BigQuery analytics config for route {spec.name!r} is missing query keys: "
                f"{sorted(missing)!r}."
            )

        if extra := config_keys - spec_keys:
            raise exc.configuration(
                f"BigQuery analytics config for route {spec.name!r} has unknown query keys: "
                f"{sorted(extra)!r}."
            )

        if spec.ingest is not None and self.resolved_ingest_relation() is None:
            raise exc.configuration(
                f"BigQuery analytics config for route {spec.name!r} requires "
                "ingest_relation (or legacy ingest_table) when AnalyticsSpec.ingest is set."
            )

        if self.tenant_aware:
            assert_tenant_param_referenced(
                {str(k): v.sql for k, v in self.queries.items()},
                pattern=r"@tenant\b",
                placeholder_hint="@tenant",
                integration="BigQuery",
                route=str(spec.name),
            )
