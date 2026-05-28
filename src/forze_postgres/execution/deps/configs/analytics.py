"""Postgres analytics execution configs."""

from typing import TYPE_CHECKING, Any, Mapping

import attrs

from forze.base.exceptions import exc
from forze.base.primitives import StrKey

from ._mapping import frozen_mapping

if TYPE_CHECKING:
    from forze.application.contracts.analytics import AnalyticsSpec

# ----------------------- #


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

    schema: str = "public"
    """PostgreSQL schema for ``ingest_table``."""

    ingest_table: str | None = None
    """Table name for analytics ingest append."""

    max_append_rows: int = 10_000
    """Maximum rows per ``append`` call."""

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

        if spec.ingest is not None and self.ingest_table is None:
            raise exc.configuration(
                f"Postgres analytics config for route {spec.name!r} requires ingest_table "
                "when AnalyticsSpec.ingest is set."
            )
