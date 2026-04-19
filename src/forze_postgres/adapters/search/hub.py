"""Multi-leg hub search: one hub projection, per-leg index heaps (engine-pluggable legs)."""

from __future__ import annotations

from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from collections.abc import Mapping, Sequence
from typing import Any, Final, Literal, Protocol, TypeVar, final, overload

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
from ._fts_sql import (
    FtsGroupLetter,
    fts_effective_group_weights,
    fts_match_predicate,
    fts_rank_cd_expr,
    fts_rank_cd_weight_array,
    fts_resolve_tsvector_expr,
    fts_tsquery_expr,
)
from ._options import prepare_hub_search_options
from ._pgroonga_sql import pgroonga_match_clause, pgroonga_score_rank_expr

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
    engine: Literal["pgroonga", "fts"] = "pgroonga"
    fts_groups: dict[FtsGroupLetter, Sequence[str]] | None = attrs.field(default=None)
    """Required when :attr:`engine` is ``fts`` (same semantics as :class:`PostgresFTSSearchAdapterV2`)."""


# ....................... #


class HubSearchLegEngine(Protocol):
    """Builds heap ``WHERE``, rank column, and parameters for one hub leg."""

    async def build_leg(
        self,
        leg: HubLegRuntime,
        *,
        introspector: PostgresIntrospector,
        index_alias: str,
        query: str,
        options: SearchOptions | None,
        score_column: str,
    ) -> tuple[sql.Composable, sql.Composable, list[Any]]:
        """Return ``(where_sql, rank_select_sql, params)`` for the leg CTE."""

        ...


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PgroongaHubLegEngine(HubSearchLegEngine):
    """PGroonga hub legs: ``&@~`` match and ``pgroonga_score``."""

    async def build_leg(
        self,
        leg: HubLegRuntime,
        *,
        introspector: PostgresIntrospector,
        index_alias: str,
        query: str,
        options: SearchOptions | None,
        score_column: str,
    ) -> tuple[sql.Composable, sql.Composable, list[Any]]:
        sw, sp = await pgroonga_match_clause(
            search=leg.search,
            index_field_map=leg.index_field_map,
            index_qname=leg.index_qname,
            introspector=introspector,
            index_alias=index_alias,
            query=query,
            options=options,
        )
        rank = pgroonga_score_rank_expr(
            index_alias=index_alias,
            rank_column=score_column,
            query=query,
        )
        return sw, rank, sp


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class FtsHubLegEngine(HubSearchLegEngine):
    """Native FTS hub legs: ``tsvector @@ tsquery`` and ``ts_rank_cd``."""

    async def build_leg(
        self,
        leg: HubLegRuntime,
        *,
        introspector: PostgresIntrospector,
        index_alias: str,
        query: str,
        options: SearchOptions | None,
        score_column: str,
    ) -> tuple[sql.Composable, sql.Composable, list[Any]]:
        _ = index_alias
        groups = leg.fts_groups

        if groups is None:
            raise CoreError("FTS hub leg requires fts_groups.")

        if not query.strip():
            return (
                sql.SQL("TRUE"),
                sql.SQL("(0)::double precision AS {}").format(
                    sql.Identifier(score_column),
                ),
                [],
            )

        tsv = await fts_resolve_tsvector_expr(introspector, leg.index_qname)
        tsw_where, tsp_w = fts_tsquery_expr(query, options=options)
        tsw_rank, tsp_r = fts_tsquery_expr(query, options=options)
        sw = fts_match_predicate(tsv=tsv, tsw=tsw_where)
        gw = fts_effective_group_weights(leg.search, groups, options)
        fts_weights = fts_rank_cd_weight_array(gw)
        rank_inner = fts_rank_cd_expr(tsv=tsv, tsw=tsw_rank)
        rank_expr = sql.SQL("{} AS {}").format(
            rank_inner,
            sql.Identifier(score_column),
        )
        sp = [fts_weights, *tsp_r, *tsp_w]

        return sw, rank_expr, sp


_PGROONGA_HUB_LEG_ENGINE: Final[PgroongaHubLegEngine] = PgroongaHubLegEngine()
_FTS_HUB_LEG_ENGINE: Final[FtsHubLegEngine] = FtsHubLegEngine()


def hub_leg_engine_for(leg: HubLegRuntime) -> HubSearchLegEngine:
    """Resolve the leg engine implementation from :attr:`HubLegRuntime.engine`."""

    eng = leg.engine

    if eng == "pgroonga":
        return _PGROONGA_HUB_LEG_ENGINE

    if eng == "fts":
        return _FTS_HUB_LEG_ENGINE

    raise CoreError(f"Unsupported hub search leg engine: {eng!r}.")


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresHubSearchAdapter[M: BaseModel](
    PostgresGateway[M],
    SearchQueryPort[M],
    TxScopedPort,
):
    """Multi-leg search with a single hub row type and merged per-leg scores.

    Each leg's :attr:`~HubLegRuntime.engine` selects the implementation
    (:class:`PgroongaHubLegEngine` or :class:`FtsHubLegEngine`). Built via
    :class:`ConfigurablePostgresHubSearch` from :class:`PostgresHubSearchConfig`.
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

    async def _hub_order_by(
        self,
        sorts: QuerySortExpression | None,  # type: ignore[valid-type]
    ) -> sql.Composable | None:
        return await self.order_by_clause(sorts, table_alias=_COMBO_ALIAS)

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
        leg_options, member_weights_list = prepare_hub_search_options(
            self.hub_spec,
            options,
        )

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

        active = [
            (i, leg, member_weights_list[i])
            for i, leg in enumerate(self.members)
            if member_weights_list[i] > 0.0
        ]

        params: list[Any] = [*fp]
        leg_cte_parts: list[sql.Composable] = []
        leg_aliases = [f"lr{i}" for i in range(len(self.members))]

        for i, leg, _ in active:
            t_alias = f"t{i}"
            lr_alias = leg_aliases[i]

            sw, rank_expr, sp = await hub_leg_engine_for(leg).build_leg(
                leg,
                introspector=self.introspector,
                index_alias=t_alias,
                query=query,
                options=leg_options,
                score_column=_LEG_SCORE,
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

        hf_cols = sql.SQL(", ").join(
            sql.SQL("{}.{}").format(sql.Identifier(_HUB_CTE), sql.Identifier(f))
            for f in sorted(self.read_fields)
        )

        merge_expr: sql.Composable
        if not active:
            merge_expr = sql.SQL("(0)::double precision")
            combine_sql = sql.SQL("TRUE")

            combo_cte = sql.SQL(
                """
                ,
                {combo} AS (
                    SELECT {hf_cols}, {merge} AS {rank}
                    FROM {hf}
                    WHERE {combine}
                )
                """
            ).format(
                combo=sql.Identifier("combo"),
                hf_cols=hf_cols,
                merge=merge_expr,
                rank=sql.Identifier(_RANK),
                hf=sql.Identifier(_HUB_CTE),
                combine=combine_sql,
            )

        else:
            score_terms = [
                sql.SQL("({}) * {}").format(
                    sql.SQL("COALESCE({}.{}, 0)").format(
                        sql.Identifier(leg_aliases[i]),
                        sql.Identifier(_LEG_SCORE),
                    ),
                    sql.Literal(float(w)),
                )
                for i, _, w in active
            ]

            if self.score_merge == "max":
                merge_expr = sql.SQL("GREATEST({})").format(
                    sql.SQL(", ").join(score_terms),
                )

            else:
                merge_expr = sql.SQL("({})").format(sql.SQL(" + ").join(score_terms))

            join_parts: list[sql.Composable] = []

            for i, leg, _ in active:
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
                    for i, _, _ in active
                ]

                if self.combine == "or":
                    combine_sql = sql.SQL(" OR ").join(leg_null_checks)  # type: ignore[assignment]

                else:
                    combine_sql = sql.SQL(" AND ").join(leg_null_checks)  # type: ignore[assignment]

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
        ob = await self._hub_order_by(sorts)

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


# Backward-compatible alias (PGroonga-only name before engine pluggability).
PostgresHubPGroongaSearchAdapter = PostgresHubSearchAdapter
