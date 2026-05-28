"""ClickHouse analytics execution configs."""

from typing import TYPE_CHECKING, Any, Mapping

import attrs

from forze.base.exceptions import exc
from forze.base.primitives import StrKey, frozen_mapping

if TYPE_CHECKING:
    from forze.application.contracts.analytics import AnalyticsSpec

# ----------------------- #


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
class ClickHouseAnalyticsConfig:
    """Physical ClickHouse mapping for one :class:`~forze.application.contracts.analytics.AnalyticsSpec` route."""

    database: str
    """ClickHouse database id."""

    queries: Mapping[StrKey, ClickHouseQueryConfig] = attrs.field(
        converter=frozen_mapping,
    )
    """Named queries; keys must match ``AnalyticsSpec.queries``."""

    ingest_table: str | None = None
    """Table name for analytics ingest append."""

    max_append_rows: int = 10_000
    """Maximum rows per ``append`` call."""

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

        if spec.ingest is not None and self.ingest_table is None:
            raise exc.configuration(
                f"ClickHouse analytics config for route {spec.name!r} requires ingest_table "
                "when AnalyticsSpec.ingest is set."
            )
