"""Postgres analytics execution configs."""

from typing import TYPE_CHECKING, Any

import attrs

from forze.application.contracts.analytics import IngestSpec
from forze.application.contracts.resolution import (
    NamedResourceSpec,
    coerce_optional_named_resource_spec,
)
from forze.application.contracts.tenancy import TenantAwareIntegrationConfig
from forze.application.integrations.analytics import assert_tenant_param_referenced
from forze.base.exceptions import exc
from forze.base.primitives import MappingConverter, StrKeyMapping
from forze_postgres.kernel.relation import RelationSpec


if TYPE_CHECKING:
    from forze.application.contracts.analytics import AnalyticsSpec

# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresQueryConfig:
    """SQL for one named analytics query."""

    sql: str
    """PostgreSQL SQL with psycopg named placeholders ``%(field)s``."""

    skip_total: bool = False
    """When True, ``run_page`` skips the COUNT wrapper."""

    cursor_column: str | None = None
    """Keyset cursor column (SQL must include ``%(forze_after)s``)."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if not self.sql.strip():
            raise exc.internal("Analytics query sql must be non-empty.")


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresAnalyticsConfig(TenantAwareIntegrationConfig):
    """Physical Postgres mapping for one :class:`~forze.application.contracts.analytics.AnalyticsSpec` route.

    When ``tenant_aware`` (inherited), the adapter binds the current tenant id as the
    ``%(tenant)s`` query parameter and fails closed if no tenant is bound; every registered
    query SQL must reference that parameter (checked at wiring).
    """

    queries: StrKeyMapping[PostgresQueryConfig] = attrs.field(
        converter=MappingConverter.to_str_key_frozen,  # type: ignore[misc]
    )
    """Named queries; keys must match ``AnalyticsSpec.queries``."""

    ingest: IngestSpec | None = None
    """Append-only ingest target ``(schema, table)`` or tenant resolver (relation-level isolation)."""

    query_schema: NamedResourceSpec | None = attrs.field(
        default=None,
        converter=coerce_optional_named_resource_spec,
    )
    """Per-tenant query schema — a static name or ``(tenant_id) -> str`` resolver.

    When set, each query runs in a transaction with ``SET LOCAL search_path`` to the
    resolved (per-tenant) schema, so an unqualified table in the registered SQL resolves in
    the tenant's own schema (schema-per-tenant isolation on a shared connection). ``None``
    leaves ``search_path`` untouched (the prior behavior).
    """

    max_append_rows: int = 10_000
    """Maximum rows per ``append`` call."""

    # ....................... #

    def resolved_ingest_relation(self) -> RelationSpec | None:
        """Effective ingest relation, or ``None`` when no ingest target is configured."""

        return self.ingest.relation if self.ingest is not None else None

    # ....................... #

    def validate_against_spec(self, spec: "AnalyticsSpec[Any, Any]") -> None:
        """Ensure integration config aligns with the kernel :class:`AnalyticsSpec`."""

        spec_keys = set(spec.queries.keys())
        config_keys = set(self.queries.keys())

        missing = spec_keys - config_keys

        if missing:
            raise exc.configuration(
                f"Postgres analytics config for route {spec.name!r} is missing query keys: "
                f"{sorted(missing)!r}."
            )

        extra = config_keys - spec_keys

        if extra:
            raise exc.configuration(
                f"Postgres analytics config for route {spec.name!r} has unknown query keys: "
                f"{sorted(extra)!r}."
            )

        if spec.ingest is not None and self.resolved_ingest_relation() is None:
            raise exc.configuration(
                f"Postgres analytics config for route {spec.name!r} requires "
                "ingest_relation (or legacy ingest_table) when AnalyticsSpec.ingest is set."
            )

        if self.tenant_aware:
            assert_tenant_param_referenced(
                {str(k): v.sql for k, v in self.queries.items()},
                pattern=r"%\(tenant\)s",
                placeholder_hint="%(tenant)s",
                integration="Postgres",
                route=str(spec.name),
            )
