"""Companion ``GROUP BY`` facet queries for Postgres ranked + browse search (RFC 0006).

Facets are computed over the **full matching set** (uncapped), independent of the page
window, by reusing the same filtered/scored SQL the page query runs over. Term semantics
match the mock reference oracle: one bucket per distinct value, count-descending, capped,
NULL excluded.
"""

from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from typing import Any, Sequence

from psycopg import sql

from forze.application.contracts.base import FacetBucket, FacetResults

from ...kernel.client import PostgresClientPort

# ----------------------- #


async def fetch_pg_facets(
    client: PostgresClientPort,
    *,
    with_clause: sql.Composable | None,
    body: sql.Composable,
    params: Sequence[Any],
    table_alias: str,
    fields: Sequence[str],
    size: int,
) -> FacetResults:
    """Run one ``GROUP BY`` companion query per facet field over the matched set.

    ``body`` is the ``FROM … [WHERE …]`` fragment (a join for the ranked pipeline, a
    ``FROM proj WHERE filter`` for the empty-query browse). ``with_clause`` is the optional
    leading ``WITH`` (``None`` for the browse path). NULL values yield no bucket.
    """

    out: dict[str, tuple[FacetBucket, ...]] = {}

    for field in fields:
        col = sql.Identifier(table_alias, field)
        head = (
            sql.SQL("{with_clause} ").format(with_clause=with_clause)
            if with_clause is not None
            else sql.SQL("")
        )
        stmt = head + sql.SQL(
            """
            SELECT {col} AS facet_value, COUNT(*) AS facet_count
            {body}
            GROUP BY {col}
            HAVING {col} IS NOT NULL
            ORDER BY facet_count DESC, facet_value ASC
            LIMIT {lim}
            """
        ).format(col=col, body=body, lim=sql.Placeholder())

        rows = await client.fetch_all(
            stmt, [*params, int(size)], row_factory="dict"
        )
        out[field] = tuple(
            FacetBucket(value=row["facet_value"], count=int(row["facet_count"]))
            for row in rows
        )

    return out
