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
    combo_limit: int | None = None,
) -> int:
    """Planner estimate for filtered rows, optionally clamped to a combo top-K cap."""

    est = await introspector.estimate_filtered_rows(
        schema=schema,
        relation=relation,
        where_sql=where_sql,
        params=params,
    )

    if combo_limit is not None:
        return min(int(est), int(combo_limit))

    return int(est)


# ....................... #


def effective_search_count(options: SearchOptions | None) -> SearchCountPolicy:
    """Resolve how ranked search should populate page totals."""

    raw = (options or {}).get("search_count", "exact")

    if raw in ("exact", "approximate", "none"):
        return raw

    return "exact"
