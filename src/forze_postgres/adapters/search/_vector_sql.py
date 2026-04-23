"""pgvector distance helpers for KNN search."""

from __future__ import annotations

from typing import Literal, Sequence

from psycopg import sql

from forze.application.contracts.search import PhraseCombine
from forze.base.errors import CoreError

# ----------------------- #

VectorDistanceKind = Literal["l2", "cosine", "inner_product"]


# ....................... #


def vector_distance_op_sql(kind: VectorDistanceKind) -> sql.SQL:
    """Return the pgvector infix operator as a SQL fragment."""

    if kind == "l2":
        return sql.SQL("<->")
    if kind == "cosine":
        return sql.SQL("<=>")
    if kind == "inner_product":
        return sql.SQL("<#>")
    raise CoreError(f"Unknown vector distance kind: {kind!r}.")


# ....................... #


def vector_param_literal(values: Sequence[float]) -> str:
    """Build a string castable to ``vector`` in PostgreSQL (bracketed list)."""

    parts = [f"{float(x):.9g}" for x in values]
    return "[" + ",".join(parts) + "]"


# ....................... #


def assert_embedding_shape(values: Sequence[float], *, expect_dim: int) -> None:
    if len(values) != expect_dim:
        raise CoreError(
            f"Embedding length {len(values)} does not match expected {expect_dim} dimensions.",
        )


# ....................... #


def vector_knn_score_expr(
    *,
    index_alias: str,
    column: str,
    kind: VectorDistanceKind,
    score_name: str,
) -> sql.Composable:
    """``(-(heap_col <op> $1::vector))`` so greater values mean closer / better match."""

    heap_col = sql.SQL("{}.{}").format(
        sql.Identifier(index_alias),
        sql.Identifier(column),
    )
    return sql.SQL("(-({hc} {op} {ph}::vector)) AS {sc}").format(
        hc=heap_col,
        op=vector_distance_op_sql(kind),
        ph=sql.Placeholder(),
        sc=sql.Identifier(score_name),
    )


# ....................... #


def vector_knn_multi_score_expr(
    *,
    index_alias: str,
    column: str,
    kind: VectorDistanceKind,
    score_name: str,
    n_queries: int,
    phrase_combine: PhraseCombine = "any",
) -> sql.Composable:
    """``GREATEST`` (``any``) or ``LEAST`` (``all``) of per-query KNN negated distances."""

    if n_queries < 1:
        raise CoreError("n_queries must be at least 1.")
    heap_col = sql.SQL("{}.{}").format(
        sql.Identifier(index_alias),
        sql.Identifier(column),
    )
    op = vector_distance_op_sql(kind)
    terms = [
        sql.SQL("(-({hc} {op} {ph}::vector))").format(
            hc=heap_col,
            op=op,
            ph=sql.Placeholder(),
        )
        for _ in range(n_queries)
    ]
    combiner = "GREATEST" if phrase_combine == "any" else "LEAST"
    inner = sql.SQL(combiner + "({})").format(sql.SQL(", ").join(terms))
    return sql.SQL("{inner} AS {sc}").format(
        inner=inner,
        sc=sql.Identifier(score_name),
    )
