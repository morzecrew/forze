"""ClickHouse analytics execution configs."""

from typing import TYPE_CHECKING, Any

import attrs

from forze.application.contracts.resolution import (
    NamedResourceSpec,
    coerce_optional_named_resource_spec,
)
from forze.application.contracts.tenancy import TenantAwareIntegrationConfig
from forze.application.integrations.analytics import assert_tenant_param_referenced
from forze.base.exceptions import exc
from forze.base.primitives import MappingConverter, StrKeyMapping
from forze_clickhouse.kernel.relation import RelationSpec, coerce_relation_spec


if TYPE_CHECKING:
    from forze.application.contracts.analytics import AnalyticsSpec

# ----------------------- #


def _optional_relation_spec(value: object) -> RelationSpec | None:
    if value is None:
        return None

    return coerce_relation_spec(value)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class ClickHouseQueryConfig:
    """SQL for one named analytics query."""

    sql: str
    """ClickHouse SQL with server-side placeholders ``{field:Type}``."""

    skip_total: bool = False
    """When True, ``run_page`` skips the COUNT wrapper."""

    cursor_column: str | None = None
    """Keyset cursor column (SQL must include ``{forze_after:Type}``)."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if not self.sql.strip():
            raise exc.internal("Analytics query sql must be non-empty.")


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class ClickHouseAnalyticsConfig(TenantAwareIntegrationConfig):
    """Physical ClickHouse mapping for one :class:`~forze.application.contracts.analytics.AnalyticsSpec` route.

    When ``tenant_aware`` (inherited), the adapter binds the current tenant id as the
    ``{tenant:UUID}`` query parameter and fails closed if no tenant is bound; every
    registered query SQL must reference that parameter (checked at wiring).
    """

    database: str
    """ClickHouse database id (the default; see :attr:`query_database` for per-tenant)."""

    queries: StrKeyMapping[ClickHouseQueryConfig] = attrs.field(
        converter=MappingConverter.to_str_key_frozen,  # type: ignore[misc]
    )
    """Named queries; keys must match ``AnalyticsSpec.queries``."""

    query_database: NamedResourceSpec | None = attrs.field(
        default=None,
        converter=coerce_optional_named_resource_spec,
    )
    """Per-tenant query database — a static name or ``(tenant_id) -> str`` resolver.

    When set, the query path runs against the resolved (per-tenant) database instead of
    :attr:`database`, so an unqualified ``FROM`` in the registered SQL hits the tenant's
    own database (database-per-tenant isolation on a shared connection). ``None`` uses
    :attr:`database`.
    """

    ingest_relation: RelationSpec | None = attrs.field(
        default=None,
        converter=_optional_relation_spec,
    )
    """Ingest target ``(database, table)`` or tenant resolver (relation-level isolation)."""

    ingest_table: str | None = None
    """Legacy table name; use :attr:`ingest_relation` ``(database, table)`` instead."""

    max_append_rows: int = 10_000
    """Maximum rows per ``append`` call."""

    # ....................... #

    def resolved_ingest_relation(self) -> RelationSpec | None:
        """Effective ingest relation from :attr:`ingest_relation` or legacy fields."""

        if self.ingest_relation is not None:
            return self.ingest_relation

        if self.ingest_table is not None:
            return (self.database, self.ingest_table)

        return None

    # ....................... #

    def validate_against_spec(self, spec: "AnalyticsSpec[Any, Any]") -> None:
        spec_keys = set(spec.queries.keys())
        config_keys = set(self.queries.keys())

        missing = spec_keys - config_keys

        if missing:
            raise exc.configuration(
                f"ClickHouse analytics config for route {spec.name!r} is missing query keys: "
                f"{sorted(missing)!r}."
            )

        extra = config_keys - spec_keys

        if extra:
            raise exc.configuration(
                f"ClickHouse analytics config for route {spec.name!r} has unknown query keys: "
                f"{sorted(extra)!r}."
            )

        if spec.ingest is not None and self.resolved_ingest_relation() is None:
            raise exc.configuration(
                f"ClickHouse analytics config for route {spec.name!r} requires "
                "ingest_relation (or legacy ingest_table) when AnalyticsSpec.ingest is set."
            )

        if self.tenant_aware:
            assert_tenant_param_referenced(
                {str(k): v.sql for k, v in self.queries.items()},
                pattern=r"\{tenant:",
                placeholder_hint="{tenant:UUID}",
                integration="ClickHouse",
                route=str(spec.name),
            )
