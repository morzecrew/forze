from typing import Mapping, NotRequired, TypedDict, final

# ----------------------- #


@final
class BigQueryQueryConfig(TypedDict):
    """SQL and options for one named analytics query."""

    sql: str
    """Standard SQL template using ``@param`` names matching the spec params model."""

    maximum_bytes_billed: NotRequired[int]
    """Per-query override for maximum bytes billed."""

    skip_total: NotRequired[bool]
    """When ``True``, ``run_page`` skips the COUNT wrapper (``Page.total`` is ``None``)."""


# ....................... #


@final
class BigQueryAnalyticsConfig(TypedDict):
    """Physical BigQuery mapping for one :class:`~forze.application.contracts.analytics.AnalyticsSpec` route."""

    dataset: str
    """BigQuery dataset id."""

    queries: Mapping[str, BigQueryQueryConfig]
    """Named queries; keys must match ``AnalyticsSpec.queries``."""

    ingest_table: NotRequired[str]
    """Table id for :class:`~forze.application.contracts.analytics.AnalyticsIngestPort` append."""

    insert_id_field: NotRequired[str]
    """Optional row field used as streaming ``insertId``."""

    max_append_rows: NotRequired[int]
    """Maximum rows per ``append`` call (raises when exceeded)."""
