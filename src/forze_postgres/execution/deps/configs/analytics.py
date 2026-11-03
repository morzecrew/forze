"""Postgres analytics execution configs."""

from datetime import timedelta
from typing import TYPE_CHECKING, Any

import attrs

from forze.application.contracts.analytics import IngestSpec, coerce_optional_ingest
from forze.application.contracts.resolution import (
    NamedResourceSpec,
    coerce_optional_named_resource_spec,
)
from forze.application.contracts.tenancy import TenantAwareIntegrationConfig
from forze.application.integrations.analytics import (
    DEFAULT_ROUTE_TIMEOUT,
    assert_tenant_param_referenced,
    validate_route_timeout,
)
from forze.base.exceptions import exc
from forze.base.primitives import MappingConverter, StrKeyMapping
from forze_postgres.kernel.relation import RelationSpec

if TYPE_CHECKING:
    from forze.application.contracts.analytics import AnalyticsSpec

STATEMENT_TIMEOUT_RESOLUTION = timedelta(milliseconds=1)
"""``statement_timeout`` counts milliseconds; a shorter timeout would round up to one."""

STATEMENT_TIMEOUT_MAX = timedelta(milliseconds=2**31 - 1)
"""The largest ``statement_timeout`` Postgres accepts (``INT_MAX`` ms, about 24.8 days)."""

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

    timeout: timedelta = DEFAULT_ROUTE_TIMEOUT
    """Ceiling on how long one execution of this query may run. Always on; a per-call
    ``options["timeout"]`` may lower it, never raise it.

    Applied as ``SET LOCAL statement_timeout``, tighten-only: a lower ``statement_timeout``
    already in force (the pool's, or an invocation deadline's) still wins. Each statement
    is bounded on its own, so the count of ``run_page`` and each batch of ``run_chunked``
    get the whole timeout. From 1 ms to :data:`STATEMENT_TIMEOUT_MAX`."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if not self.sql.strip():
            raise exc.internal("Analytics query sql must be non-empty.")

        validate_route_timeout(
            self.timeout,
            resolution=STATEMENT_TIMEOUT_RESOLUTION,
            maximum=STATEMENT_TIMEOUT_MAX,
        )


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

    ingest: IngestSpec | None = attrs.field(
        default=None,
        converter=coerce_optional_ingest,
    )
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

    max_append_rows: int = 100_000
    """Maximum rows per ``append`` call — a latency and memory guard, not a protocol limit.

    It used to be both. Ingest ran as one multi-VALUES ``INSERT``, and the extended protocol
    tops out at 65 535 bind parameters, so ~6 columns × 10 000 rows already sat near a
    ceiling the server would enforce as an error. ``COPY`` has no such ceiling, and the cap
    survives for the other reason: a governed route should still refuse a surprise 10⁷-row
    call rather than discover the size at the far end of a load.

    Raising it per route is now an honest tuning decision about how much latency and memory
    one call may take, rather than negotiating with a protocol cliff.
    """

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
