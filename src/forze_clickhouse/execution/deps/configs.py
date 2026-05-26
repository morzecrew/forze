from typing import Mapping, NotRequired, TypedDict, final

# ----------------------- #


@final
class ClickHouseQueryConfig(TypedDict):
    """SQL for one named analytics query."""

    sql: str
    """ClickHouse SQL with server-side placeholders ``{field:Type}``."""

    skip_total: NotRequired[bool]
    """When ``True``, ``run_page`` skips the COUNT wrapper (``Page.total`` is ``None``)."""

    cursor_column: NotRequired[str]
    """When set, ``run_cursor`` uses keyset pagination on this column (SQL must include ``{forze_after:Type}``)."""


# ....................... #


@final
class ClickHouseAnalyticsConfig(TypedDict):
    """Physical ClickHouse mapping for one :class:`~forze.application.contracts.analytics.AnalyticsSpec` route."""

    database: str
    """ClickHouse database id."""

    queries: Mapping[str, ClickHouseQueryConfig]
    """Named queries; keys must match ``AnalyticsSpec.queries``."""

    ingest_table: NotRequired[str]
    """Table name for :class:`~forze.application.contracts.analytics.AnalyticsIngestPort` append."""

    max_append_rows: NotRequired[int]
    """Maximum rows per ``append`` call (raises when exceeded)."""
