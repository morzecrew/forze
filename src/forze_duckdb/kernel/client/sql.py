"""SQL helpers for the DuckDB analytics client.

Pagination and counting wrap the registered query SQL in an outer subquery rather
than requiring the query author to embed ``LIMIT`` / ``OFFSET`` placeholders.
Integer bounds are validated and rendered inline; named ``$param`` bindings inside
the inner query are untouched.
"""

from forze.base.exceptions import exc

# ----------------------- #

_COUNT_COLUMN = "forze_cnt"

# ....................... #


def build_count_sql(inner_sql: str) -> str:
    """Wrap *inner_sql* in ``SELECT COUNT(*)`` for total row counts."""

    stripped = inner_sql.strip().rstrip(";")

    return (
        f"SELECT COUNT(*) AS {_COUNT_COLUMN} "  # nosec B608
        f"FROM ({stripped}) AS forze_analytics_subq"
    )


# ....................... #


def apply_limit_offset(
    sql: str,
    limit: int | None,
    offset: int | None,
) -> str:
    """Wrap *sql* with ``LIMIT`` / ``OFFSET`` when a window is requested."""

    if limit is None and offset is None:
        return sql

    stripped = sql.strip().rstrip(";")
    clause = ""

    if limit is not None:
        if limit < 0:
            raise exc.internal("Analytics pagination 'limit' must be >= 0.")

        clause += f" LIMIT {int(limit)}"

    if offset is not None:
        if offset < 0:
            raise exc.internal("Analytics pagination 'offset' must be >= 0.")

        clause += f" OFFSET {int(offset)}"

    return f"SELECT * FROM ({stripped}) AS forze_page_subq{clause}"  # nosec B608
