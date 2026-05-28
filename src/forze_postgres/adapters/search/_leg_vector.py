"""Shared vector leg scoring for simple adapters and hub legs."""

from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from typing import Any

from psycopg import sql

from forze.application.contracts.embeddings import EmbeddingsProviderPort
from forze.application.contracts.search import SearchOptions, effective_phrase_combine

from ...kernel.catalog.introspect import PostgresIntrospector
from ._vector_sql import (
    VectorDistanceKind,
    assert_embedding_shape,
    vector_knn_multi_score_expr,
    vector_knn_score_expr,
    vector_param_literal,
)

# ----------------------- #


async def build_vector_leg(
    *,
    embedder: EmbeddingsProviderPort,
    introspector: PostgresIntrospector,
    index_alias: str,
    vector_column: str,
    vector_distance: VectorDistanceKind,
    embedding_dimensions: int,
    queries: tuple[str, ...],
    options: SearchOptions | None,
    score_column: str,
) -> tuple[sql.Composable, sql.Composable, list[Any]]:
    """Build heap ``WHERE`` (always ``TRUE``), KNN rank fragment, and vector parameters.

    Parameter order: one ``vector`` literal per query (``vector_param_literal``).
    """

    _ = introspector
    combine = effective_phrase_combine(options)

    if not queries:
        return (
            sql.SQL("TRUE"),
            sql.SQL("(0)::double precision AS {}").format(
                sql.Identifier(score_column),
            ),
            [],
        )

    sw = sql.SQL("TRUE")

    if len(queries) == 1:
        one = await embedder.embed_one(queries[0], input_kind="query")
        assert_embedding_shape(one, expect_dim=embedding_dimensions)

        rank = vector_knn_score_expr(
            index_alias=index_alias,
            column=vector_column,
            kind=vector_distance,
            score_name=score_column,
        )
        leg_params = [vector_param_literal(one)]

    else:
        vecs = await embedder.embed(queries, input_kind="query")

        for vec in vecs:
            assert_embedding_shape(vec, expect_dim=embedding_dimensions)

        rank = vector_knn_multi_score_expr(
            index_alias=index_alias,
            column=vector_column,
            kind=vector_distance,
            score_name=score_column,
            n_queries=len(vecs),
            phrase_combine=combine,
        )
        leg_params = [vector_param_literal(v) for v in vecs]

    return sw, rank, leg_params
