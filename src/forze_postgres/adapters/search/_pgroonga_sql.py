"""Shared PGroonga SQL for single-table search (v2) and hub legs."""

from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from collections.abc import Mapping
from typing import Any

from psycopg import sql

from forze.application.contracts.search import SearchOptions, SearchSpec
from forze.base.errors import CoreError

from ...kernel.gateways import PostgresQualifiedName
from ...kernel.introspect import PostgresIntrospector
from ._utils import calculate_effective_field_weights

# ----------------------- #


def pgroonga_heap_column_names(
    search: SearchSpec[Any],
    index_field_map: Mapping[str, str] | None,
) -> list[str]:
    """Resolve heap column names for :class:`SearchSpec` fields."""

    if index_field_map is None:
        return list(search.fields)

    return [index_field_map.get(f, f) for f in search.fields]


# ....................... #


async def pgroonga_match_clause(
    *,
    search: SearchSpec[Any],
    index_field_map: Mapping[str, str] | None,
    index_qname: PostgresQualifiedName,
    introspector: PostgresIntrospector,
    index_alias: str,
    query: str,
    options: SearchOptions | None,
) -> tuple[sql.Composable, list[Any]]:
    """Heap-side ``&@~`` match and parameters (single-column or ``ARRAY[...]`` index)."""

    options = options or {}
    query = query.strip()
    index = index_qname.string()
    ia = index_alias

    if not query:
        return sql.SQL("TRUE"), []

    params: list[Any] = [query, index]

    q_ph = sql.Placeholder()
    idx_ph = sql.Placeholder()
    r_ph = sql.Placeholder()
    w_ph = sql.Placeholder()

    eff_float = calculate_effective_field_weights(search, options)
    eff_weights = {f: int(w * 100) for f, w in eff_float.items()}
    heap_cols = pgroonga_heap_column_names(search, index_field_map)

    if len(heap_cols) != len(eff_weights):
        raise CoreError("Search field / weight alignment error.")

    weights = [eff_weights[f] for f in search.fields]
    use_fuzzy = options.get("fuzzy", False)

    if search.fuzzy is not None:
        ratio = search.fuzzy.get("max_distance_ratio", 0.34)

    else:
        ratio = 0.34

    index_info = await introspector.get_index_info(
        index=index_qname.name,
        schema=index_qname.schema,
    )

    if index_info.expr is None or ("ARRAY" not in index_info.expr.upper()):
        col = heap_cols[0]
        text_expr = sql.SQL("coalesce({}::text, '')").format(
            sql.Identifier(ia, col),
        )

        if use_fuzzy:
            params.append(float(ratio))
            cond = sql.SQL(
                (
                    "pgroonga_condition({}::text, "
                    "index_name => {}::text, "
                    "fuzzy_max_distance_ratio => {}::float4)"
                )
            ).format(q_ph, idx_ph, r_ph)

        else:
            cond = sql.SQL(
                "pgroonga_condition({}::text, index_name => {}::text)"
            ).format(q_ph, idx_ph)

        return sql.SQL("{} &@~ {}").format(text_expr, cond), params

    array_expr = sql.SQL("(ARRAY[{}])").format(
        sql.SQL(", ").join(
            sql.SQL("coalesce({}::text, '')").format(sql.Identifier(ia, c))
            for c in heap_cols
        )
    )
    params.append(weights)

    if use_fuzzy:
        params.append(float(ratio))
        cond = sql.SQL(
            (
                "pgroonga_condition({}::text, "
                "index_name => {}::text, "
                "weights => {}::int[], "
                "fuzzy_max_distance_ratio => {}::float4)"
            )
        ).format(q_ph, idx_ph, w_ph, r_ph)

    else:
        cond = sql.SQL(
            (
                "pgroonga_condition({}::text, "
                "index_name => {}::text, "
                "weights => {}::int[])"
            )
        ).format(q_ph, idx_ph, w_ph)

    return sql.SQL("{} &@~ {}").format(array_expr, cond), params


# ....................... #


def pgroonga_score_rank_expr(
    *,
    index_alias: str,
    rank_column: str,
    query: str,
) -> sql.Composable:
    """``SELECT`` fragment for per-row rank: ``pgroonga_score`` or zero when query is empty."""

    if not query.strip():
        return sql.SQL("(0)::double precision AS {}").format(
            sql.Identifier(rank_column),
        )

    return sql.SQL("{} AS {}").format(
        sql.SQL("pgroonga_score({})").format(sql.Identifier(index_alias)),
        sql.Identifier(rank_column),
    )
