"""PGroonga hub search: multiple indexed heaps, one hub projection (OR/AND legs)."""

from __future__ import annotations

from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from collections.abc import Mapping, Sequence
from typing import Any, Final, Literal, TypeVar, final, overload

import attrs
from psycopg import sql
from pydantic import BaseModel

from forze.application.contracts.query import (
    PaginationExpression,
    QueryFilterExpression,
    QuerySortExpression,
)
from forze.application.contracts.search import (
    HubSearchSpec,
    SearchOptions,
    SearchQueryPort,
    SearchSpec,
)
from forze.application.contracts.tx import TxScopedPort, TxScopeKey
from forze.base.errors import CoreError
from forze.base.primitives import JsonDict
from forze.base.serialization import pydantic_validate_many

from ...kernel.gateways import PostgresGateway, PostgresQualifiedName
from ...kernel.introspect import PostgresIntrospector
from ..txmanager import PostgresTxScopeKey
from ._utils import calculate_effective_field_weights

# ----------------------- #

T = TypeVar("T", bound=BaseModel)

_HUB_CTE: Final[str] = "hf"
_HUB_ROW_ALIAS: Final[str] = "h"
_COMBO_ALIAS: Final[str] = "comb"
_RANK: Final[str] = "_hub_rank"
_LEG_SCORE: Final[str] = "s"
_LEG_EID: Final[str] = "eid"

# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class HubLegRuntime:
    """Resolved leg: :class:`SearchSpec` plus Postgres index/heap wiring."""

    search: SearchSpec[Any]
    index_qname: PostgresQualifiedName
    index_heap_qname: PostgresQualifiedName
    hub_fk_column: str
    heap_pk_column: str
    index_field_map: Mapping[str, str] | None = attrs.field(default=None)


# ....................... #


def _heap_columns(
    search: SearchSpec[Any],
    index_field_map: Mapping[str, str] | None,
) -> list[str]:
    if index_field_map is None:
        return list(search.fields)

    return [index_field_map.get(f, f) for f in search.fields]


# ....................... #


async def _pgroonga_match_clause(
    *,
    search: SearchSpec[Any],
    index_field_map: Mapping[str, str] | None,
    index_qname: PostgresQualifiedName,
    introspector: PostgresIntrospector,
    index_alias: str,
    query: str,
    options: SearchOptions | None,
) -> tuple[sql.Composable, list[Any]]:
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
    heap_cols = _heap_columns(search, index_field_map)

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


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresHubPGroongaSearchAdapter[M: BaseModel](
    PostgresGateway[M],
    SearchQueryPort[M],
    TxScopedPort,
):
    """Multi-leg PGroonga search with a single hub row type.

    Built via :class:`ConfigurablePostgresHubSearch` so index/heap wiring and
    merge policy stay in :class:`PostgresHubSearchConfig`.
    """

    hub_spec: HubSearchSpec[M]
    members: Sequence[HubLegRuntime]
    combine: Literal["or", "and"] = "or"
    score_merge: Literal["max", "sum"] = "max"

    tx_scope: TxScopeKey = attrs.field(default=PostgresTxScopeKey, init=False)

    # ....................... #

    def _hub_select_list(self) -> sql.Composable:
        return sql.SQL(", ").join(
            sql.Identifier(_HUB_ROW_ALIAS, f) for f in sorted(self.read_fields)
        )

    # ....................... #

    def _hub_order_by(
        self,
        sorts: QuerySortExpression | None,  # type: ignore[valid-type]
    ) -> sql.Composable | None:
        if not sorts:
            return None

        parts: list[sql.Composable] = []

        for field, order in sorts.items():
            parts.append(
                sql.SQL("{} {}").format(
                    sql.Identifier(_COMBO_ALIAS, field),
                    sql.SQL(order.upper()),
                )
            )

        return sql.SQL(", ").join(parts)

    # ....................... #

    @overload
    async def search(
        self,
        query: str,
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        options: SearchOptions | None = ...,
        return_type: None = ...,
        return_fields: None = ...,
    ) -> tuple[list[M], int]: ...

    @overload
    async def search(
        self,
        query: str,
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        options: SearchOptions | None = ...,
        return_type: type[T],
        return_fields: None = ...,
    ) -> tuple[list[T], int]: ...

    @overload
    async def search(
        self,
        query: str,
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        options: SearchOptions | None = ...,
        return_type: None = ...,
        return_fields: Sequence[str],
    ) -> tuple[list[JsonDict], int]: ...

    async def search(
        self,
        query: str,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        return_type: type[T] | None = None,
        return_fields: Sequence[str] | None = None,
    ) -> tuple[list[M] | list[T] | list[JsonDict], int]:
        fw, fp = await self.where_clause(filters)

        hub_cte = sql.SQL(
            """
            {hub_cte} AS (
                SELECT {hub_cols}
                FROM {hub_rel} {ha}
                WHERE {fw}
            )
            """
        ).format(
            hub_cte=sql.Identifier(_HUB_CTE),
            hub_cols=self._hub_select_list(),
            hub_rel=self.source_qname.ident(),
            ha=sql.Identifier(_HUB_ROW_ALIAS),
            fw=fw,
        )

        params: list[Any] = [*fp]
        leg_cte_parts: list[sql.Composable] = []
        leg_aliases = [f"lr{i}" for i in range(len(self.members))]

        for i, leg in enumerate(self.members):
            t_alias = f"t{i}"
            lr_alias = leg_aliases[i]

            sw, sp = await _pgroonga_match_clause(
                search=leg.search,
                index_field_map=leg.index_field_map,
                index_qname=leg.index_qname,
                introspector=self.introspector,
                index_alias=t_alias,
                query=query,
                options=options,
            )
            params.extend(sp)

            cand_sub = sql.SQL(
                """
                ( SELECT DISTINCT {fk} AS cand_id FROM {hf} WHERE {fk} IS NOT NULL ) {csub}
                """
            ).format(
                fk=sql.Identifier(_HUB_CTE, leg.hub_fk_column),
                hf=sql.Identifier(_HUB_CTE),
                csub=sql.Identifier(f"csub{i}"),
            )

            join_on = sql.SQL("{} = {}").format(
                sql.Identifier(t_alias, leg.heap_pk_column),
                sql.Identifier(f"csub{i}", "cand_id"),
            )

            if query.strip():
                rank_expr = sql.SQL("{} AS {}").format(
                    sql.SQL("pgroonga_score({})").format(sql.Identifier(t_alias)),
                    sql.Identifier(_LEG_SCORE),
                )
            else:
                rank_expr = sql.SQL("(0)::double precision AS {})").format(
                    sql.Identifier(_LEG_SCORE),
                )

            sel_pk = sql.SQL("{} AS {}").format(
                sql.SQL("{}.{}").format(
                    sql.Identifier(t_alias),
                    sql.Identifier(leg.heap_pk_column),
                ),
                sql.Identifier(_LEG_EID),
            )
            leg_cte = sql.SQL(
                """
                ,
                {lr} AS (
                    SELECT {sel_pk}, {rank_expr}
                    FROM {heap} {t}
                    INNER JOIN {cand} ON ({join_on})
                    WHERE {sw}
                )
                """
            ).format(
                lr=sql.Identifier(lr_alias),
                sel_pk=sel_pk,
                rank_expr=rank_expr,
                heap=leg.index_heap_qname.ident(),
                t=sql.Identifier(t_alias),
                sw=sw,
                cand=cand_sub,
                join_on=join_on,
            )
            leg_cte_parts.append(leg_cte)

        score_terms = [
            sql.SQL("COALESCE({}.{}, 0)").format(
                sql.Identifier(leg_aliases[i]),
                sql.Identifier(_LEG_SCORE),
            )
            for i in range(len(self.members))
        ]
        if self.score_merge == "max":
            merge_expr = sql.SQL("GREATEST({})").format(sql.SQL(", ").join(score_terms))

        else:
            merge_expr = sql.SQL("({})").format(sql.SQL(" + ").join(score_terms))

        join_parts: list[sql.Composable] = []

        for i, leg in enumerate(self.members):
            join_parts.append(
                sql.SQL("LEFT JOIN {} ON {} = {}").format(
                    sql.Identifier(leg_aliases[i]),
                    sql.Identifier(_HUB_CTE, leg.hub_fk_column),
                    sql.Identifier(leg_aliases[i], _LEG_EID),
                )
            )

        leg_joins = sql.SQL(" ").join(join_parts)

        if not query.strip():
            combine_sql = sql.SQL("TRUE")

        else:
            leg_null_checks = [
                sql.SQL("{} IS NOT NULL").format(
                    sql.Identifier(leg_aliases[i], _LEG_EID),
                )
                for i in range(len(self.members))
            ]

            if self.combine == "or":
                combine_sql = sql.SQL(" OR ").join(leg_null_checks)  # type: ignore[assignment]

            else:
                combine_sql = sql.SQL(" AND ").join(leg_null_checks)  # type: ignore[assignment]

        hf_cols = sql.SQL(", ").join(
            sql.SQL("{}.{}").format(sql.Identifier(_HUB_CTE), sql.Identifier(f))
            for f in sorted(self.read_fields)
        )

        combo_cte = sql.SQL(
            """
            ,
            {combo} AS (
                SELECT {hf_cols}, {merge} AS {rank}
                FROM {hf}
                {leg_joins}
                WHERE {combine}
            )
            """
        ).format(
            combo=sql.Identifier("combo"),
            hf_cols=hf_cols,
            merge=merge_expr,
            rank=sql.Identifier(_RANK),
            hf=sql.Identifier(_HUB_CTE),
            leg_joins=leg_joins,
            combine=combine_sql,
        )

        order_parts: list[sql.Composable] = [
            sql.SQL("{} DESC NULLS LAST").format(
                sql.Identifier(_COMBO_ALIAS, _RANK),
            )
        ]
        ob = self._hub_order_by(sorts)

        if ob is not None:
            order_parts.append(ob)

        order_sql = sql.SQL(", ").join(order_parts)

        with_clause = sql.SQL("WITH {}{}{}").format(
            hub_cte,
            sql.SQL("").join(leg_cte_parts),
            combo_cte,
        )

        count_stmt = sql.SQL(
            """
            {with_clause}
            SELECT COUNT(*) FROM {combo} {ca}
            """
        ).format(
            with_clause=with_clause,
            combo=sql.Identifier("combo"),
            ca=sql.Identifier(_COMBO_ALIAS),
        )

        total = int(await self.client.fetch_value(count_stmt, params, default=0))

        if total == 0:
            return [], total

        cols = self.return_clause(
            return_type,
            return_fields,
            table_alias=_COMBO_ALIAS,
        )

        data_stmt = sql.SQL(
            """
            {with_clause}
            SELECT {cols} FROM {combo} {ca}
            ORDER BY {order}
            """
        ).format(
            with_clause=with_clause,
            cols=cols,
            combo=sql.Identifier("combo"),
            ca=sql.Identifier(_COMBO_ALIAS),
            order=order_sql,
        )

        pagination = pagination or {}
        limit = pagination.get("limit")
        offset = pagination.get("offset")

        if limit is not None:
            data_stmt += sql.SQL(" LIMIT {}").format(sql.Placeholder())
            params.append(int(limit))

        if offset is not None:
            data_stmt += sql.SQL(" OFFSET {}").format(sql.Placeholder())
            params.append(int(offset))

        rows = await self.client.fetch_all(data_stmt, params, row_factory="dict")

        if return_type is not None:
            return pydantic_validate_many(return_type, rows), total

        if return_fields is not None:
            return [{k: r.get(k, None) for k in return_fields} for r in rows], total

        return pydantic_validate_many(self.model_type, rows), total
