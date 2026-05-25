"""BigQuery client configuration and query results."""

from datetime import timedelta
from typing import final

import attrs

from forze.base.primitives import JsonDict

# ----------------------- #

_DEFAULT_TIMEOUT = timedelta(seconds=60)

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class BigQueryConfig:
    """Optional overrides for :class:`BigQueryClient`."""

    timeout: timedelta = attrs.field(default=_DEFAULT_TIMEOUT)
    """Default HTTP timeout for BigQuery API calls."""

    use_legacy_sql: bool = False
    """When ``True``, use legacy SQL (default Standard SQL)."""

    maximum_bytes_billed: int | None = None
    """Default maximum bytes billed per query."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class BigQueryQueryResult:
    """Parsed outcome of a BigQuery query execution."""

    rows: list[JsonDict]
    """Result rows as plain dictionaries."""

    total_rows: int | None = None
    """Total rows in the full result set when reported by BigQuery."""

    page_token: str | None = None
    """Opaque page token for the next page, if any."""

    job_id: str | None = None
    """Job id when results require follow-up paging via jobs API."""
