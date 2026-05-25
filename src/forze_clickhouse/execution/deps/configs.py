from typing import Mapping, NotRequired, TypedDict, final

# ----------------------- #


@final
class ClickHouseQueryConfig(TypedDict):
    """SQL for one named analytics query."""

    sql: str
    """ClickHouse SQL with server-side placeholders ``{field:Type}``."""


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
