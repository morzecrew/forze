"""Shared FTS SQL fragments for Postgres search adapters (v2, hub legs)."""

from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from statistics import mean
from typing import Any, Literal, Sequence

from psycopg import sql

from forze.application.contracts.search import SearchOptions, SearchSpec
from forze.base.errors import CoreError

from ...kernel.gateways import PostgresQualifiedName
from ...kernel.introspect import PostgresIntrospector
from ._utils import calculate_effective_field_weights

# ----------------------- #

FtsGroupLetter = Literal["A", "B", "C", "D"]
"""One of the four Postgres FTS weight labels."""

# ....................... #


async def fts_resolve_tsvector_expr(
    introspector: PostgresIntrospector,
    index_qname: PostgresQualifiedName,
) -> sql.Composable:
    """Load the indexed ``tsvector`` expression from the catalog."""

    index_info = await introspector.get_index_info(
        index=index_qname.name,
        schema=index_qname.schema,
    )

    if not index_info.expr:
        raise CoreError("Unable to infer tsvector expression from index definition.")

    # NOTE: expr is raw SQL fragment from Postgres catalog; we still treat it as config.
    return sql.SQL(index_info.expr)  # pyright: ignore[reportArgumentType]


# ....................... #


def fts_tsquery_expr(
    query: str,
    *,
    options: SearchOptions | None = None,
) -> tuple[sql.Composable, list[Any]]:
    """Build ``websearch_to_tsquery`` (and params) for the user query."""

    _ = options  # reserved for future search modes (phrase / exact / plain)
    query = query.strip()
    params: list[Any] = [query]

    return sql.SQL("websearch_to_tsquery({}::text)").format(sql.Placeholder()), params


# ....................... #


def fts_effective_group_weights(
    spec: SearchSpec[Any],
    fts_groups: dict[FtsGroupLetter, Sequence[str]],
    options: SearchOptions | None = None,
    *,
    alpha: float = 0.7,
) -> dict[FtsGroupLetter, float]:
    """Map per-field weights into per-FTS-letter weights."""

    weights = calculate_effective_field_weights(spec, options)

    group_weights: dict[FtsGroupLetter, list[float]] = {
        k: [weights[x] for x in v] for k, v in fts_groups.items()
    }

    agg_weights: dict[FtsGroupLetter, float] = {}

    for g, w in group_weights.items():
        non_zero = [x for x in w if x > 0]
        non_zero_term = mean(non_zero) if non_zero else 0.0
        agg_weights[g] = alpha * max(w) + (1 - alpha) * non_zero_term

    return agg_weights


# ....................... #


def fts_rank_cd_weight_array(gw: dict[FtsGroupLetter, float]) -> list[float]:
    """``ts_rank_cd`` expects ``float4[]`` in D, C, B, A order."""

    return [
        gw.get("D", 0.0),
        gw.get("C", 0.0),
        gw.get("B", 0.0),
        gw.get("A", 0.0),
    ]


# ....................... #


def fts_rank_cd_expr(
    *,
    tsv: sql.Composable,
    tsw: sql.Composable,
) -> sql.Composable:
    """``ts_rank_cd`` call sharing the same ``tsw`` composable as the match predicate."""

    return sql.SQL("ts_rank_cd({weights}::float4[], ({tsv}), ({tsw}))").format(
        weights=sql.Placeholder(),
        tsv=tsv,
        tsw=tsw,
    )


# ....................... #


def fts_match_predicate(
    *,
    tsv: sql.Composable,
    tsw: sql.Composable,
) -> sql.Composable:
    """Boolean ``(tsvector) @@ (tsquery)`` fragment."""

    return sql.SQL("(({tsv}) @@ ({tsw}))").format(tsv=tsv, tsw=tsw)
