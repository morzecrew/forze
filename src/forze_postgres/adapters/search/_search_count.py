"""Search total-count policy helpers for Postgres ranked search."""

from __future__ import annotations

from typing import Any, Literal

from psycopg import sql

from forze.application.contracts.search import SearchOptions

SearchCountPolicy = Literal["exact", "approximate", "none"]

# ----------------------- #


async def resolve_ranked_approximate_total(
    *,
    introspector: Any,
    schema: str,
    relation: str,
    where_sql: sql.Composable,
    params: list[Any],
    alias: str | None = None,
) -> int:
    """Planner estimate of the rows ``where_sql`` matches, for a page with no search text.

    A text predicate is left to the caller: the planner estimates a PGroonga match at a fixed
    selectivity whatever the query, so a ranked page counts its capped candidates instead.
    """

    est = await introspector.estimate_filtered_rows(
        schema=schema,
        relation=relation,
        where_sql=where_sql,
        params=params,
        alias=alias,
    )

    return int(est)


# ....................... #


def effective_search_count(options: SearchOptions | None) -> SearchCountPolicy:
    """Resolve how ranked search should populate page totals."""

    raw = (options or {}).get("search_count", "exact")

    if raw in ("exact", "approximate", "none"):
        return raw

    return "exact"
