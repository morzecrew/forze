"""Postgres analytics execution configs."""

from typing import TYPE_CHECKING, Any, Mapping

import attrs

from forze.base.exceptions import exc
from forze.base.primitives import StrKey, frozen_mapping
from forze_postgres.kernel.relation import RelationSpec, coerce_relation_spec

if TYPE_CHECKING:
    from forze.application.contracts.analytics import AnalyticsSpec

# ----------------------- #


def _optional_relation_spec(value: object) -> RelationSpec | None:
    if value is None:
        return None

    return coerce_relation_spec(value)


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
class PostgresAnalyticsConfig:
    """Physical Postgres mapping for one :class:`~forze.application.contracts.analytics.AnalyticsSpec` route."""

    queries: Mapping[StrKey, PostgresQueryConfig] = attrs.field(
        converter=frozen_mapping
    )
    """Named queries; keys must match ``AnalyticsSpec.queries``."""

    ingest_relation: RelationSpec | None = attrs.field(
        default=None,
        converter=_optional_relation_spec,
    )
    """Ingest target ``(schema, table)`` or tenant resolver (relation-level isolation)."""

    schema: str = "public"
    """Legacy schema for :attr:`ingest_table` when :attr:`ingest_relation` is omitted."""

    ingest_table: str | None = None
    """Legacy table name; use :attr:`ingest_relation` ``(schema, table)`` instead."""

    max_append_rows: int = 10_000
    """Maximum rows per ``append`` call."""

    # ....................... #

    def resolved_ingest_relation(self) -> RelationSpec | None:
        """Effective ingest relation from :attr:`ingest_relation` or legacy fields."""

        if self.ingest_relation is not None:
            return self.ingest_relation

        if self.ingest_table is not None:
            return (self.schema, self.ingest_table)

        return None

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
