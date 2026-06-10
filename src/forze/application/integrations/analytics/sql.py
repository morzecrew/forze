"""Shared SQL builders for warehouse analytics adapters.

Counting and pagination wrap the registered query SQL in an outer subquery
rather than appending clauses, so registered queries that already end in
``LIMIT`` / ``OFFSET`` (or a trailing ``;``) keep working. ``ORDER BY``
inside the inner query still controls result order through the wrap.

The SQL text composed here comes from registered analytics query config
(developer-authored identifiers and statements), never from user input, so
bandit's SQL-injection check (B608) is suppressed at the f-string sites.
"""

from pydantic import BaseModel

from forze.base.exceptions import exc

# ----------------------- #

COUNT_COLUMN = "forze_cnt"
"""Column alias produced by :func:`build_count_sql`."""

# ....................... #


def parameters_from_model(params: BaseModel) -> dict[str, object]:
    """Build named query parameters from a Pydantic model."""

    return params.model_dump()


# ....................... #


def build_count_sql(inner_sql: str, *, count_expr: str = "COUNT(*)") -> str:
    """Wrap *inner_sql* in ``SELECT {count_expr}`` for total row counts.

    :param inner_sql: Registered query SQL to count over.
    :param count_expr: Backend count expression (e.g. ``count()`` for ClickHouse).
    """

    stripped = inner_sql.strip().rstrip(";")

    return (
        f"SELECT {count_expr} AS {COUNT_COLUMN} "  # nosec B608 - registered config SQL, not user input
        f"FROM ({stripped}) AS forze_analytics_subq"
    )


# ....................... #


def apply_limit_offset(
    sql: str,
    *,
    limit: int | None = None,
    offset: int | None = None,
) -> str:
    """Wrap *sql* in a subquery applying ``LIMIT`` / ``OFFSET`` when requested.

    Wrapping (instead of appending) keeps registered queries that already end
    in their own ``LIMIT`` clause valid.
    """

    stripped = sql.strip().rstrip(";")

    if limit is None and offset is None:
        return stripped

    clause = ""

    if limit is not None:
        if limit < 0:
            raise exc.internal("Analytics pagination 'limit' must be >= 0.")

        clause += f" LIMIT {int(limit)}"

    if offset is not None:
        if offset < 0:
            raise exc.internal("Analytics pagination 'offset' must be >= 0.")

        clause += f" OFFSET {int(offset)}"

    return f"SELECT * FROM ({stripped}) AS forze_page_subq{clause}"  # nosec B608 - registered config SQL, not user input
