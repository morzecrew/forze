"""Companion ``GROUP BY`` facet queries for Postgres ranked + browse search.

Facets are computed over the **full matching set** (uncapped), independent of the page
window, by reusing the same filtered/scored SQL the page query runs over. Term semantics
match the mock reference oracle: one bucket per distinct value, count-descending, capped,
NULL excluded.
"""

from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from collections.abc import Sequence
from typing import Any

from psycopg import sql

from forze.application.contracts.search import FacetBucket, FacetResults
from forze.domain.constants import ID_FIELD

from ...kernel.client import PostgresClientPort
from ...kernel.gateways import PostgresQualifiedName

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

        rows = await client.fetch_all(stmt, [*params, int(size)], row_factory="dict")
        out[field] = tuple(
            FacetBucket(value=row["facet_value"], count=int(row["facet_count"])) for row in rows
        )

    return out


# ....................... #


async def fetch_hub_facets(
    client: PostgresClientPort,
    *,
    with_clause: sql.Composable,
    count_relation: str,
    combo_alias: str,
    read_relation: PostgresQualifiedName,
    params: Sequence[Any],
    fields: Sequence[str],
    size: int,
) -> FacetResults:
    """Term facets over the merged hub matched set (``sql`` execution).

    Joins the uncapped merged candidate relation (``count_relation`` in *with_clause*) to the
    hub read relation by id so the companion can ``GROUP BY`` the facet column, which lives on
    the read row, not the thin candidate pipeline. Counts are over the full matched set,
    independent of the page window — the same set the hub total counts.
    """

    read_alias = "fct"
    body = sql.SQL("FROM {combo} {ca} JOIN {read} {ra} ON {ra}.{idf} = {ca}.{idf}").format(
        combo=sql.Identifier(count_relation),
        ca=sql.Identifier(combo_alias),
        read=read_relation.ident(),
        ra=sql.Identifier(read_alias),
        idf=sql.Identifier(ID_FIELD),
    )

    return await fetch_pg_facets(
        client,
        with_clause=with_clause,
        body=body,
        params=params,
        table_alias=read_alias,
        fields=fields,
        size=size,
    )
