"""Shared PGroonga leg scoring for simple adapters and hub legs."""

from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from typing import Any, Literal, Mapping

from psycopg import sql

from forze.application.contracts.search import (
    SearchOptions,
    SearchSpec,
    effective_phrase_combine,
)

from ...kernel.catalog.introspect import PostgresIntrospector
from ...kernel.gateways import PostgresQualifiedName
from ._pgroonga_sql import (
    pgroonga_match_clause,
    pgroonga_phrase_match_text,
    pgroonga_score_rank_expr,
)

# ----------------------- #


async def build_pgroonga_leg(
    *,
    introspector: PostgresIntrospector,
    index_qname: PostgresQualifiedName,
    search: SearchSpec[Any],
    index_field_map: Mapping[str, str] | None,
    index_alias: str,
    queries: tuple[str, ...],
    options: SearchOptions | None,
    score_column: str,
    pgroonga_score_version: Literal["v1", "v2"] = "v2",
) -> tuple[sql.Composable, sql.Composable, list[Any]]:
    """Build heap ``WHERE``, rank ``SELECT`` fragment, and match parameters.

    Parameter order: PGroonga match clause placeholders (query, index name, …).
    """

    if not queries:
        return (
            sql.SQL("TRUE"),
            sql.SQL("(0)::double precision AS {}").format(
                sql.Identifier(score_column),
            ),
            [],
        )

    mq = pgroonga_phrase_match_text(
        queries,
        combine=effective_phrase_combine(options),
    )
    sw, sp = await pgroonga_match_clause(
        search=search,
        index_field_map=index_field_map,
        index_qname=index_qname,
        introspector=introspector,
        index_alias=index_alias,
        query=mq,
        options=options,
    )
    rank = pgroonga_score_rank_expr(
        index_alias=index_alias,
        rank_column=score_column,
        query=mq,
        score_version=pgroonga_score_version,
    )
    return sw, rank, sp
