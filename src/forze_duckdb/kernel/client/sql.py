"""SQL helpers for the DuckDB analytics client.

Pagination and counting wrap the registered query SQL in an outer subquery rather
than requiring the query author to embed ``LIMIT`` / ``OFFSET`` placeholders.
Integer bounds are validated and rendered inline; named ``$param`` bindings inside
the inner query are untouched.

Thin wrappers over the shared analytics SQL builders in
:mod:`forze.application.integrations.analytics.sql`.
"""

from forze.application.integrations.analytics.sql import (
    build_count_sql as build_count_sql,  # thin re-export of the shared builder
)
from forze.application.integrations.analytics.sql import (
    apply_limit_offset as _apply_limit_offset,
)

# ----------------------- #

__all__ = [
    "apply_limit_offset",
    "build_count_sql",
]

# ....................... #


def apply_limit_offset(
    sql: str,
    limit: int | None,
    offset: int | None,
) -> str:
    """Wrap *sql* with ``LIMIT`` / ``OFFSET`` when a window is requested."""

    return _apply_limit_offset(sql, limit=limit, offset=offset)
