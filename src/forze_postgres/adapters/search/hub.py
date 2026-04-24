"""Hub search: one hub projection and one or more per-leg index heaps (engine-pluggable legs)."""

from __future__ import annotations

from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from typing import (
    Any,
    Final,
    Literal,
    Mapping,
    Protocol,
    Sequence,
    TypeVar,
    final,
    overload,
)

import attrs
from psycopg import sql
from pydantic import BaseModel

from forze.application.contracts.base import (
    CountlessPage,
    CursorPage,
    Page,
    page_from_limit_offset,
)
from forze.application.contracts.embeddings import EmbeddingsProviderPort
from forze.application.contracts.query import (
    CursorPaginationExpression,
    PaginationExpression,
    QueryFilterExpression,
    QuerySortExpression,
    decode_keyset_v1,
    encode_keyset_v1,
    normalize_sorts_with_id,
    row_value_for_sort_key,
)
from forze.application.contracts.search import (
    HubSearchSpec,
    SearchOptions,
    SearchQueryPort,
    SearchResultSnapshotOptions,
    SearchResultSnapshotPort,
    SearchSpec,
    effective_phrase_combine,
    normalize_search_queries,
)
from forze.application.contracts.tx import TxScopedPort, TxScopeKey
from forze.base.errors import CoreError
from forze.base.primitives import JsonDict
from forze.base.serialization import pydantic_validate_many
from forze.domain.constants import ID_FIELD

from ...kernel.gateways import PostgresGateway, PostgresQualifiedName
from ...kernel.hub_fk_columns import normalize_hub_fk_columns
from ...kernel.introspect import PostgresIntrospector
from ...kernel.query.nested import sort_key_expr
from ...pagination import build_seek_condition
from ..txmanager import PostgresTxScopeKey
from ._fts_sql import (
    FtsGroupLetter,
    fts_effective_group_weights,
    fts_match_predicate,
    fts_rank_cd_expr,
    fts_rank_cd_weight_array,
    fts_resolve_tsvector_expr,
    fts_tsquery_expr,
    fts_tsquery_expr_conjunction,
    fts_tsquery_expr_disjunction,
)
from ._cursor_keyset import cursor_return_fields_for_select
from ._options import prepare_hub_search_options
from ._pgroonga_sql import (
    pgroonga_match_clause,
    pgroonga_phrase_match_text,
    pgroonga_score_rank_expr,
)
from ._vector_sql import (
    VectorDistanceKind,
    assert_embedding_shape,
    vector_knn_multi_score_expr,
    vector_knn_score_expr,
    vector_param_literal,
)
from .federated_snapshot import effective_snapshot_max_ids
from .result_snapshot_ops import (
    hub_search_fingerprint,
    put_simple_result_snapshot,
    read_hub_result_snapshot,
    should_write_result_snapshot,
    snapshot_sql_pagination,
)

# ----------------------- #

T = TypeVar("T", bound=BaseModel)

_HUB_CTE: Final[str] = "hf"
_HUB_ROW_ALIAS: Final[str] = "h"
_COMBO_ALIAS: Final[str] = "comb"
_RANK: Final[str] = "_hub_rank"
_LEG_SCORE: Final[str] = "s"
_LEG_EID: Final[str] = "eid"
# Groonga v2 needs physical row ids: projected when a pgroonga leg uses same_heap_as_hub.
_HUB_GROONGA_TABLEOID: Final[str] = "_hub_groonga_tableoid"
_HUB_GROONGA_CTID: Final[str] = "_hub_groonga_ctid"

# ....................... #


def _empty_vector_embedders() -> dict[int, EmbeddingsProviderPort]:
    return {}


# ....................... #


def _hub_leg_candidate_subquery(leg: HubLegRuntime, csub_alias: str) -> sql.Composable:
    """Distinct heap PK candidates from the hub CTE (UNION when multiple hub FKs)."""

    cols = leg.hub_fk_columns
    hf = sql.Identifier(_HUB_CTE)
    csub = sql.Identifier(csub_alias)

    if len(cols) == 1:
        fk = sql.Identifier(_HUB_CTE, cols[0])
        return sql.SQL(
            "( SELECT DISTINCT {fk} AS cand_id FROM {hf} WHERE {fk} IS NOT NULL ) {csub}",
        ).format(fk=fk, hf=hf, csub=csub)

    branches = [
        sql.SQL(
            "( SELECT DISTINCT {fk} AS cand_id FROM {hf} WHERE {fk} IS NOT NULL )",
        ).format(fk=sql.Identifier(_HUB_CTE, col), hf=hf)
        for col in cols
    ]
    unioned = sql.SQL(" UNION ").join(branches)
    return sql.SQL("({u}) {csub}").format(u=unioned, csub=csub)


def _hub_leg_equi_pick_join(
    leg: HubLegRuntime,
    leg_cte_alias: str,
    pick_alias: str,
) -> sql.Composable:
    """Equi-join the single hub FK to the leg (``eid``, ``s``).

    Inlines ``LEFT JOIN (SELECT DISTINCT ON (eid) …) … ON (hf.fk = eid)`` so the
    planner can use a hash/merge plan. For multiple hub FKs, use
    :func:`_hub_leg_leg_u_cte` and :func:`_hub_leg_multi_equi_pick_join`.
    """

    (col,) = leg.hub_fk_columns

    lr = sql.Identifier(leg_cte_alias)
    pick = sql.Identifier(pick_alias)
    t = sql.Identifier("t")
    t_eid = sql.SQL("{}.{}").format(t, sql.Identifier(_LEG_EID))
    t_s = sql.SQL("{}.{}").format(t, sql.Identifier(_LEG_SCORE))
    hf_fk = sql.Identifier(_HUB_CTE, col)

    return sql.SQL(
        "LEFT JOIN ( "
        "SELECT DISTINCT ON ({t_eid}) {t_eid} AS {eid}, {t_s} AS {sc} "
        "FROM {lr} {t} "
        "ORDER BY {t_eid}, {t_s} DESC NULLS LAST"
        ") {pick} ON ({hf_fk} = {t_eid_qualified})"
    ).format(
        t_eid=t_eid,
        eid=sql.Identifier(_LEG_EID),
        t_s=t_s,
        sc=sql.Identifier(_LEG_SCORE),
        lr=lr,
        t=t,
        pick=pick,
        hf_fk=hf_fk,
        t_eid_qualified=sql.SQL("{}.{}").format(
            pick,
            sql.Identifier(_LEG_EID),
        ),
    )


def _hub_leg_leg_u_cte(leg_cte_alias: str, u_cte_name: str) -> sql.Composable:
    """Deduplicate a leg to one ``(eid, s)`` per ``eid`` (best ``s``), for multi-FK joins."""

    lr = sql.Identifier(leg_cte_alias)
    lr_u = sql.Identifier(u_cte_name)
    t = sql.Identifier("t")
    t_eid = sql.SQL("{}.{}").format(t, sql.Identifier(_LEG_EID))
    t_s = sql.SQL("{}.{}").format(t, sql.Identifier(_LEG_SCORE))
    return sql.SQL(
        """
        ,
        {lr_u} AS (
            SELECT DISTINCT ON ({t_eid}) {t_eid} AS {eid}, {t_s} AS {sc}
            FROM {lr} {t}
            ORDER BY {t_eid}, {t_s} DESC NULLS LAST
        )
        """
    ).format(
        lr_u=lr_u,
        lr=lr,
        t=t,
        t_eid=t_eid,
        t_s=t_s,
        eid=sql.Identifier(_LEG_EID),
        sc=sql.Identifier(_LEG_SCORE),
    )


def _hub_leg_multi_equi_pick_join(
    leg: HubLegRuntime,
    leg_u_cte: str,
    base_pick_prefix: str,
) -> sql.Composable:
    """K ``LEFT JOIN``s from hub FK columns to deduplicated ``leg_u`` (OR + best score in SELECT)."""

    leg_u = sql.Identifier(leg_u_cte)
    parts: list[sql.Composable] = []
    for j, col in enumerate(leg.hub_fk_columns):
        pick = sql.Identifier(f"{base_pick_prefix}_{j}")
        hf_fk = sql.Identifier(_HUB_CTE, col)
        t_eid_q = sql.SQL("{}.{}").format(pick, sql.Identifier(_LEG_EID))
        parts.append(
            sql.SQL("LEFT JOIN {leg_u} {pick} ON ({hf_fk} = {eid})").format(
                leg_u=leg_u,
                pick=pick,
                hf_fk=hf_fk,
                eid=t_eid_q,
            ),
        )
    return sql.SQL(" ").join(parts)


def _hub_leg_merge_coalesce(leg: HubLegRuntime, leg_index: int) -> sql.Composable:
    """Per-leg match score: single FK uses one join; multi-FK uses ``GREATEST`` of K joins."""

    if len(leg.hub_fk_columns) == 1:
        return sql.SQL("COALESCE({}.{}, 0)").format(
            sql.Identifier(f"lp{leg_index}"),
            sql.Identifier(_LEG_SCORE),
        )
    br = [
        sql.SQL("COALESCE({}.{}, 0)").format(
            sql.Identifier(f"lp{leg_index}_{j}"),
            sql.Identifier(_LEG_SCORE),
        )
        for j in range(len(leg.hub_fk_columns))
    ]
    return sql.SQL("GREATEST({})").format(sql.SQL(", ").join(br))


def _hub_leg_merge_matched(leg: HubLegRuntime, leg_index: int) -> sql.Composable:
    """Whether this leg matched: non-null leg ``eid`` on any FK join branch."""

    if len(leg.hub_fk_columns) == 1:
        return sql.SQL("{} IS NOT NULL").format(
            sql.SQL("{}.{}").format(
                sql.Identifier(f"lp{leg_index}"),
                sql.Identifier(_LEG_EID),
            ),
        )
    eid_null = [
        sql.SQL("{} IS NOT NULL").format(
            sql.SQL("{}.{}").format(
                sql.Identifier(f"lp{leg_index}_{j}"),
                sql.Identifier(_LEG_EID),
            ),
        )
        for j in range(len(leg.hub_fk_columns))
    ]
    return sql.SQL("({})").format(sql.SQL(" OR ").join(eid_null))


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class HubLegRuntime:
    """Resolved leg: :class:`SearchSpec` plus Postgres index/heap wiring."""

    search: SearchSpec[Any]
    index_qname: PostgresQualifiedName
    index_heap_qname: PostgresQualifiedName
    hub_fk_columns: tuple[str, ...] = attrs.field(converter=normalize_hub_fk_columns)
    heap_pk_column: str
    index_field_map: Mapping[str, str] | None = attrs.field(default=None)
    pgroonga_score_version: Literal["v1", "v2"] | None = attrs.field(default=None)
    """``v1`` / ``v2`` :func:`pgroonga_score` form when :attr:`engine` is ``pgroonga``; else ``None``."""

    engine: Literal["pgroonga", "fts", "vector"] = "pgroonga"
    fts_groups: dict[FtsGroupLetter, Sequence[str]] | None = attrs.field(default=None)
    """Required when :attr:`engine` is ``fts`` (same semantics as :class:`PostgresFTSSearchAdapterV2`)."""

    vector_column: str | None = None
    """Heap column of type ``vector`` when :attr:`engine` is ``vector``."""

    vector_distance: VectorDistanceKind = "l2"
    """pgvector distance family when :attr:`engine` is ``vector``."""

    embedding_dimensions: int | None = None
    """Expected query embedding length for ``vector`` legs."""

    same_heap_as_hub: bool = False
    """If True, the leg is evaluated on the hub CTE (``hf``) without joining the heap again."""

    def __attrs_post_init__(self) -> None:
        if self.engine == "vector":
            if not self.vector_column or self.embedding_dimensions is None:
                raise CoreError(
                    "Vector hub leg requires vector_column and embedding_dimensions.",
                )


# ....................... #


class HubSearchLegEngine(Protocol):
    """Builds heap ``WHERE``, rank column, and parameters for one hub leg."""

    async def build_leg(
        self,
        leg: HubLegRuntime,
        *,
        introspector: PostgresIntrospector,
        index_alias: str,
        queries: tuple[str, ...],
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
        queries: tuple[str, ...],
        options: SearchOptions | None,
        score_column: str,
    ) -> tuple[sql.Composable, sql.Composable, list[Any]]:
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
            search=leg.search,
            index_field_map=leg.index_field_map,
            index_qname=leg.index_qname,
            introspector=introspector,
            index_alias=index_alias,
            query=mq,
            options=options,
        )
        rank = pgroonga_score_rank_expr(
            index_alias=index_alias,
            rank_column=score_column,
            query=mq,
            score_version=leg.pgroonga_score_version or "v2",
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
        queries: tuple[str, ...],
        options: SearchOptions | None,
        score_column: str,
    ) -> tuple[sql.Composable, sql.Composable, list[Any]]:
        _ = index_alias
        groups = leg.fts_groups

        if groups is None:
            raise CoreError("FTS hub leg requires fts_groups.")

        if not queries:
            return (
                sql.SQL("TRUE"),
                sql.SQL("(0)::double precision AS {}").format(
                    sql.Identifier(score_column),
                ),
                [],
            )

        tsv = await fts_resolve_tsvector_expr(introspector, leg.index_qname)
        if len(queries) == 1:
            tsw_where, tsp_w = fts_tsquery_expr(queries[0], options=options)
            tsw_rank, tsp_r = fts_tsquery_expr(queries[0], options=options)
        else:
            fn = (
                fts_tsquery_expr_disjunction
                if effective_phrase_combine(options) == "any"
                else fts_tsquery_expr_conjunction
            )
            tsw_where, tsp_w = fn(queries, options=options)
            tsw_rank, tsp_r = fn(queries, options=options)
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


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class VectorHubLegEngine(HubSearchLegEngine):
    """Vector hub legs: KNN score on a ``vector`` heap column."""

    embedder: EmbeddingsProviderPort

    async def build_leg(
        self,
        leg: HubLegRuntime,
        *,
        introspector: PostgresIntrospector,
        index_alias: str,
        queries: tuple[str, ...],
        options: SearchOptions | None,
        score_column: str,
    ) -> tuple[sql.Composable, sql.Composable, list[Any]]:
        _ = introspector
        combine = effective_phrase_combine(options)
        if leg.engine != "vector" or leg.vector_column is None:
            raise CoreError("VectorHubLegEngine requires a vector hub leg.")
        if not queries:
            return (
                sql.SQL("TRUE"),
                sql.SQL("(0)::double precision AS {}").format(
                    sql.Identifier(score_column),
                ),
                [],
            )

        edim = leg.embedding_dimensions

        if edim is None:
            raise CoreError("embedding_dimensions is required for vector engine.")

        sw = sql.SQL("TRUE")
        if len(queries) == 1:
            one = await self.embedder.embed_one(queries[0], input_kind="query")
            assert_embedding_shape(one, expect_dim=edim)
            rank = vector_knn_score_expr(
                index_alias=index_alias,
                column=leg.vector_column,
                kind=leg.vector_distance,
                score_name=score_column,
            )
            sp = [vector_param_literal(one)]
        else:
            vecs = await self.embedder.embed(queries, input_kind="query")
            for vec in vecs:
                assert_embedding_shape(vec, expect_dim=edim)
            rank = vector_knn_multi_score_expr(
                index_alias=index_alias,
                column=leg.vector_column,
                kind=leg.vector_distance,
                score_name=score_column,
                n_queries=len(vecs),
                phrase_combine=combine,
            )
            sp = [vector_param_literal(v) for v in vecs]
        return sw, rank, sp


_PGROONGA_HUB_LEG_ENGINE: Final[PgroongaHubLegEngine] = PgroongaHubLegEngine()
_FTS_HUB_LEG_ENGINE: Final[FtsHubLegEngine] = FtsHubLegEngine()


def hub_leg_engine_for(
    leg: HubLegRuntime,
    *,
    vector_embedder: EmbeddingsProviderPort | None = None,
) -> HubSearchLegEngine:
    """Resolve the leg engine implementation from :attr:`HubLegRuntime.engine`."""

    eng = leg.engine

    if eng == "pgroonga":
        return _PGROONGA_HUB_LEG_ENGINE

    if eng == "fts":
        return _FTS_HUB_LEG_ENGINE

    if eng == "vector":
        if vector_embedder is None:
            raise CoreError("Vector hub leg requires an embeddings provider.")
        return VectorHubLegEngine(embedder=vector_embedder)

    raise CoreError(f"Unsupported hub search leg engine: {eng!r}.")


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresHubSearchAdapter[M: BaseModel](
    PostgresGateway[M],
    SearchQueryPort[M],
    TxScopedPort,
):
    """Search over a hub row type with one or more legs and merged per-leg scores.

    Each leg's :attr:`~HubLegRuntime.engine` selects the implementation
    (PGroonga, FTS, or :class:`VectorHubLegEngine`). Built via
    :class:`ConfigurablePostgresHubSearch` from :class:`PostgresHubSearchConfig`.
    """

    hub_spec: HubSearchSpec[M]
    members: Sequence[HubLegRuntime]
    vector_embedders: Mapping[int, EmbeddingsProviderPort] = attrs.field(
        factory=_empty_vector_embedders,
    )
    """Per-leg index → embedder for :attr:`~HubLegRuntime.engine` ``vector`` legs."""

    snapshot_store: SearchResultSnapshotPort | None = None
    """Optional store for ordered hub row keys to accelerate paged reads."""

    combine: Literal["or", "and"] = "or"
    score_merge: Literal["max", "sum"] = "max"

    tx_scope: TxScopeKey = attrs.field(default=PostgresTxScopeKey, init=False)

    # ....................... #

    def _hub_select_list(self, *, include_groonga_sys: bool) -> sql.Composable:
        base = sql.SQL(", ").join(
            sql.Identifier(_HUB_ROW_ALIAS, f) for f in sorted(self.read_fields)
        )
        if not include_groonga_sys:
            return base
        ha = sql.Identifier(_HUB_ROW_ALIAS)
        ext = sql.SQL("{}, {}").format(
            sql.SQL("{}.tableoid AS {}").format(
                ha, sql.Identifier(_HUB_GROONGA_TABLEOID)
            ),
            sql.SQL("{}.ctid AS {}").format(ha, sql.Identifier(_HUB_GROONGA_CTID)),
        )
        return sql.SQL("{}, {}").format(base, ext)

    # ....................... #

    async def _hub_order_by(
        self,
        sorts: QuerySortExpression | None,  # type: ignore[valid-type]
    ) -> sql.Composable | None:
        return await self.order_by_clause(sorts, table_alias=_COMBO_ALIAS)

    # ....................... #

    async def _hub_order_sql_for_search(
        self,
        do_legs: bool,
        sorts: QuerySortExpression | None,  # type: ignore[valid-type]
    ) -> sql.Composable:
        if do_legs:
            order_parts: list[sql.Composable] = [
                sql.SQL("{} DESC NULLS LAST").format(
                    sql.Identifier(_COMBO_ALIAS, _RANK),
                )
            ]
            ob = await self._hub_order_by(sorts)
            if ob is not None:
                order_parts.append(ob)
            return sql.SQL(", ").join(order_parts)

        ob = await self._hub_order_by(sorts)
        if ob is not None:
            order_parts = [ob]
        elif ID_FIELD in self.read_fields:
            order_parts = [
                sql.SQL("{} ASC").format(
                    sql.Identifier(_COMBO_ALIAS, ID_FIELD),
                ),
            ]
        else:
            first = sorted(self.read_fields)[0]
            order_parts = [
                sql.SQL("{} ASC").format(
                    sql.Identifier(_COMBO_ALIAS, first),
                ),
            ]
        return sql.SQL(", ").join(order_parts)

    async def _hub_build_with_clause(
        self,
        *,
        query_terms: tuple[str, ...],
        filters: QueryFilterExpression | None,  # type: ignore[valid-type]
        leg_options: SearchOptions | None,
        member_weights_list: Sequence[float],
    ) -> tuple[sql.Composable, list[Any], bool]:
        fw, fp = await self.where_clause(filters)

        active = [
            (i, leg, member_weights_list[i])
            for i, leg in enumerate(self.members)
            if member_weights_list[i] > 0.0
        ]

        do_legs = bool(query_terms) and bool(active)
        need_groonga_sys = do_legs and any(
            leg.same_heap_as_hub and leg.engine == "pgroonga" for _, leg, _ in active
        )

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
            hub_cols=self._hub_select_list(include_groonga_sys=need_groonga_sys),
            hub_rel=self.source_qname.ident(),
            ha=sql.Identifier(_HUB_ROW_ALIAS),
            fw=fw,
        )

        params: list[Any] = [*fp]
        leg_cte_parts: list[sql.Composable] = []
        leg_aliases = [f"lr{i}" for i in range(len(self.members))]

        if do_legs:
            for i, leg, _ in active:
                t_alias = _HUB_ROW_ALIAS if leg.same_heap_as_hub else f"t{i}"
                lr_alias = leg_aliases[i]

                v_emb = self.vector_embedders.get(i) if leg.engine == "vector" else None
                sw, rank_expr, sp = await hub_leg_engine_for(
                    leg,
                    vector_embedder=v_emb,
                ).build_leg(
                    leg,
                    introspector=self.introspector,
                    index_alias=t_alias,
                    queries=query_terms,
                    options=leg_options,
                    score_column=_LEG_SCORE,
                )
                params.extend(sp)

                if leg.same_heap_as_hub and leg.engine == "pgroonga" and query_terms:
                    rank_expr = sql.SQL("pgroonga_score({}.{}, {}.{}) AS {}").format(
                        sql.Identifier(t_alias),
                        sql.Identifier(_HUB_GROONGA_TABLEOID),
                        sql.Identifier(t_alias),
                        sql.Identifier(_HUB_GROONGA_CTID),
                        sql.Identifier(_LEG_SCORE),
                    )

                sel_pk = sql.SQL("{} AS {}").format(
                    sql.SQL("{}.{}").format(
                        sql.Identifier(t_alias),
                        sql.Identifier(leg.heap_pk_column),
                    ),
                    sql.Identifier(_LEG_EID),
                )

                if leg.same_heap_as_hub:
                    leg_cte = sql.SQL(
                        """
                        ,
                        {lr} AS (
                            SELECT {sel_pk}, {rank_expr}
                            FROM {hf} {t}
                            WHERE {sw}
                        )
                        """
                    ).format(
                        lr=sql.Identifier(lr_alias),
                        sel_pk=sel_pk,
                        rank_expr=rank_expr,
                        hf=sql.Identifier(_HUB_CTE),
                        t=sql.Identifier(t_alias),
                        sw=sw,
                    )
                else:
                    cand_sub = _hub_leg_candidate_subquery(leg, f"csub{i}")
                    join_on = sql.SQL("{} = {}").format(
                        sql.Identifier(t_alias, leg.heap_pk_column),
                        sql.Identifier(f"csub{i}", "cand_id"),
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
                if len(leg.hub_fk_columns) > 1:
                    leg_cte_parts.append(
                        _hub_leg_leg_u_cte(lr_alias, f"{lr_alias}_u"),
                    )

        hf_cols = sql.SQL(", ").join(
            sql.SQL("{}.{}").format(sql.Identifier(_HUB_CTE), sql.Identifier(f))
            for f in sorted(self.read_fields)
        )

        merge_expr: sql.Composable
        if not do_legs:
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
                    _hub_leg_merge_coalesce(leg, i),
                    sql.Literal(float(w)),
                )
                for i, leg, w in active
            ]

            if self.score_merge == "max":
                merge_expr = sql.SQL("GREATEST({})").format(
                    sql.SQL(", ").join(score_terms),
                )

            else:
                merge_expr = sql.SQL("({})").format(sql.SQL(" + ").join(score_terms))

            join_parts: list[sql.Composable] = []

            for i, leg, _ in active:
                if len(leg.hub_fk_columns) == 1:
                    join_parts.append(
                        _hub_leg_equi_pick_join(leg, leg_aliases[i], f"lp{i}"),
                    )
                else:
                    join_parts.append(
                        _hub_leg_multi_equi_pick_join(
                            leg,
                            f"{leg_aliases[i]}_u",
                            f"lp{i}",
                        ),
                    )

            leg_joins = sql.SQL(" ").join(join_parts)

            leg_null_checks = [_hub_leg_merge_matched(leg, i) for i, leg, _ in active]

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

        with_clause = sql.SQL("WITH {}{}{}").format(
            hub_cte,
            sql.SQL("").join(leg_cte_parts),
            combo_cte,
        )
        return with_clause, params, do_legs

    def _hub_cursor_key_spec(
        self,
        *,
        do_legs: bool,
        sorts: QuerySortExpression | None,  # type: ignore[valid-type]
    ) -> list[tuple[str, str]]:
        if not do_legs:
            if not sorts:
                if ID_FIELD in self.read_fields:
                    return [(ID_FIELD, "asc")]
                first = sorted(self.read_fields)[0]
                return [(first, "asc")]
            return list(normalize_sorts_with_id(sorts))

        spec: list[tuple[str, str]] = [(_RANK, "desc")]
        if sorts:
            for field, direction in sorts.items():
                d = str(direction).lower()
                if d not in ("asc", "desc"):
                    raise CoreError(
                        f"Invalid sort direction in hub cursor: {direction!r}"
                    )
                spec.append((field, d))
        have = {k for k, _ in spec}
        if ID_FIELD not in have:
            id_dir = "asc"
            if sorts:
                dirs = {str(v).lower() for v in sorts.values()}
                if len(dirs) == 1:
                    id_dir = next(iter(dirs))
            spec.append((ID_FIELD, id_dir))
        return spec

    @staticmethod
    def _hub_cursor_order_sql(
        exprs: list[sql.Composable],
        sort_keys: list[str],
        directions: list[str],
        *,
        flip: bool,
    ) -> sql.Composable:
        parts: list[sql.Composable] = []
        for ex, d_raw, sk in zip(exprs, directions, sort_keys, strict=True):
            d = ("desc" if d_raw == "asc" else "asc") if flip else d_raw
            if sk == _RANK:
                if d == "desc":
                    parts.append(sql.SQL("{} DESC NULLS LAST").format(ex))
                else:
                    parts.append(sql.SQL("{} ASC NULLS FIRST").format(ex))
            else:
                suf = "ASC" if d == "asc" else "DESC"
                parts.append(sql.SQL("{} {}").format(ex, sql.SQL(suf)))
        return sql.SQL(", ").join(parts)

    # ....................... #

    @overload
    async def search(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        options: SearchOptions | None = ...,
        snapshot: SearchResultSnapshotOptions | None = ...,
        return_type: None = ...,
        return_fields: None = ...,
        return_count: Literal[False] = ...,
    ) -> CountlessPage[M]: ...

    @overload
    async def search(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        options: SearchOptions | None = ...,
        snapshot: SearchResultSnapshotOptions | None = ...,
        return_type: type[T],
        return_fields: None = ...,
        return_count: Literal[False] = ...,
    ) -> CountlessPage[T]: ...

    @overload
    async def search(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        options: SearchOptions | None = ...,
        snapshot: SearchResultSnapshotOptions | None = ...,
        return_type: None = ...,
        return_fields: Sequence[str],
        return_count: Literal[False] = ...,
    ) -> CountlessPage[JsonDict]: ...

    @overload
    async def search(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        options: SearchOptions | None = ...,
        snapshot: SearchResultSnapshotOptions | None = ...,
        return_type: None = ...,
        return_fields: None = ...,
        return_count: Literal[True] = ...,
    ) -> Page[M]: ...

    @overload
    async def search(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        options: SearchOptions | None = ...,
        snapshot: SearchResultSnapshotOptions | None = ...,
        return_type: type[T],
        return_fields: None = ...,
        return_count: Literal[True] = ...,
    ) -> Page[T]: ...

    @overload
    async def search(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        options: SearchOptions | None = ...,
        snapshot: SearchResultSnapshotOptions | None = ...,
        return_type: None = ...,
        return_fields: Sequence[str],
        return_count: Literal[True] = ...,
    ) -> Page[JsonDict]: ...

    async def search(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        snapshot: SearchResultSnapshotOptions | None = None,
        return_type: type[T] | None = None,
        return_fields: Sequence[str] | None = None,
        return_count: bool = False,
    ) -> (
        CountlessPage[M]
        | CountlessPage[T]
        | CountlessPage[JsonDict]
        | Page[M]
        | Page[T]
        | Page[JsonDict]
    ):
        terms = normalize_search_queries(query)

        leg_options, member_weights_list = prepare_hub_search_options(
            self.hub_spec,
            options,
        )

        rs_spec = self.hub_spec.result_snapshot
        members_weighted: list[tuple[str, float]] = [
            (self.hub_spec.members[i].name, float(member_weights_list[i]))
            for i in range(len(self.hub_spec.members))
        ]
        fp_fingerprint = hub_search_fingerprint(
            query,
            filters,
            sorts,
            spec_name=self.hub_spec.name,
            members_weighted=members_weighted,
            score_merge=str(self.score_merge),
            combine=str(self.combine),
        )
        if self.snapshot_store is not None and rs_spec is not None:
            read_page = await read_hub_result_snapshot(
                store=self.snapshot_store,
                rs_spec=rs_spec,
                snap_opt=snapshot,
                fp_computed=fp_fingerprint,
                model_type=self.model_type,
                pagination=dict(pagination or {}),
                return_type=return_type,
                return_fields=return_fields,
                return_count=return_count,
            )

            if read_page is not None:
                return read_page

        with_clause, params, do_legs = await self._hub_build_with_clause(
            query_terms=terms,
            filters=filters,
            leg_options=leg_options,
            member_weights_list=member_weights_list,
        )

        order_sql = await self._hub_order_sql_for_search(do_legs, sorts)

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

        total = 0
        if return_count:
            total = int(await self.client.fetch_value(count_stmt, params, default=0))
            if total == 0:
                return page_from_limit_offset(
                    [],
                    pagination or {},
                    total=0,
                )

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

        want_sn = (
            self.snapshot_store is not None
            and rs_spec is not None
            and should_write_result_snapshot(snapshot, rs_spec)
        )
        max_nh = effective_snapshot_max_ids(snapshot, rs_spec) if want_sn else 0
        sql_limit, sql_offset, page_limit = snapshot_sql_pagination(
            want_sn, max_nh, dict(pagination)
        )

        if sql_limit is not None:
            data_stmt += sql.SQL(" LIMIT {}").format(sql.Placeholder())
            params.append(int(sql_limit))

        if want_sn:
            data_stmt += sql.SQL(" OFFSET {}").format(sql.Placeholder())
            params.append(int(sql_offset))

        elif pagination.get("offset") is not None:
            data_stmt += sql.SQL(" OFFSET {}").format(sql.Placeholder())
            params.append(int(pagination.get("offset") or 0))

        rows = await self.client.fetch_all(data_stmt, params, row_factory="dict")

        handle_h = None
        if want_sn and self.snapshot_store is not None and rs_spec is not None:
            plh = len(rows)
            handle_h = await put_simple_result_snapshot(
                self.snapshot_store,
                pydantic_validate_many(self.model_type, rows),
                snap_opt=snapshot,
                rs_spec=rs_spec,
                fp_computed=fp_fingerprint,
                pool_len_before_cap=plh,
            )
            u_h = int(pagination.get("offset") or 0)
            rows = rows[u_h : u_h + page_limit]

        if return_type is not None:
            v = pydantic_validate_many(return_type, rows)

            if return_count:
                return page_from_limit_offset(
                    v,
                    pagination,
                    total=total,
                    result_snapshot=handle_h,
                )

            return page_from_limit_offset(
                v,
                pagination,
                total=None,
                result_snapshot=handle_h,
            )

        if return_fields is not None:
            raw = [{k: r.get(k, None) for k in return_fields} for r in rows]
            if return_count:
                return page_from_limit_offset(
                    raw,
                    pagination,
                    total=total,
                    result_snapshot=handle_h,
                )
            return page_from_limit_offset(
                raw,
                pagination,
                total=None,
                result_snapshot=handle_h,
            )

        m = pydantic_validate_many(self.model_type, rows)
        if return_count:
            return page_from_limit_offset(
                m,
                pagination,
                total=total,
                result_snapshot=handle_h,
            )
        return page_from_limit_offset(
            m,
            pagination,
            total=None,
            result_snapshot=handle_h,
        )

    # ....................... #

    @overload
    async def search_with_cursor(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        options: SearchOptions | None = ...,
        return_type: None = ...,
        return_fields: None = ...,
    ) -> CursorPage[M]: ...

    @overload
    async def search_with_cursor(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        options: SearchOptions | None = ...,
        return_type: type[T],
        return_fields: None = ...,
    ) -> CursorPage[T]: ...

    @overload
    async def search_with_cursor(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        options: SearchOptions | None = ...,
        return_type: None = ...,
        return_fields: Sequence[str],
    ) -> CursorPage[JsonDict]: ...

    async def search_with_cursor(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        return_type: type[T] | None = None,
        return_fields: Sequence[str] | None = None,
    ) -> CursorPage[M] | CursorPage[T] | CursorPage[JsonDict]:
        """Keyset pagination over the hub ``combo`` row (filter-only or ranked legs).

        **Browse (empty query, no sorts):** Orders by ``id`` ascending when the read
        model defines that field (same as explicit ``sorts`` with ``id`` ascending and
        :meth:`search`). Without an ``id`` field, falls back to the lexicographically
        first read-model field only (ties may be unstable).

        **Ranked:** With active legs and a non-empty query, ordering is merged
        ``_hub_rank`` DESC NULLS LAST, optional ``sorts`` (including ``id`` if given),
        then an ``id`` tie-breaker when omitted.

        With ``return_fields``, list only the columns you want in each hit; keyset
        columns (including ``_hub_rank`` when legs are active) are selected
        internally and stripped from the response.
        """

        terms = normalize_search_queries(query)

        leg_options, member_weights_list = prepare_hub_search_options(
            self.hub_spec,
            options,
        )

        c = dict(cursor or {})

        if c.get("after") and c.get("before"):
            raise CoreError(
                "Cursor pagination: pass at most one of 'after' or 'before'",
            )

        lim: int = 10 if c.get("limit") is None else int(c["limit"])  # type: ignore[arg-type, assignment, call-overload]

        if lim < 1:
            raise CoreError("Cursor pagination 'limit' must be positive")

        use_after = c.get("after") is not None
        use_before = c.get("before") is not None

        with_clause, params, do_legs = await self._hub_build_with_clause(
            query_terms=terms,
            filters=filters,
            leg_options=leg_options,
            member_weights_list=member_weights_list,
        )

        key_spec = self._hub_cursor_key_spec(do_legs=do_legs, sorts=sorts)
        sort_keys = [k for k, _ in key_spec]
        directions = [d for _, d in key_spec]

        types = await self.column_types()
        exprs: list[sql.Composable] = []

        for k in sort_keys:
            if k == _RANK:
                exprs.append(sql.Identifier(_COMBO_ALIAS, _RANK))
            else:
                exprs.append(
                    sort_key_expr(
                        field=k,
                        column_types=types,
                        model_type=self.model_type,
                        nested_field_hints=self.nested_field_hints,
                        table_alias=_COMBO_ALIAS,
                    ),
                )

        where_fin: sql.Composable = sql.SQL("TRUE")

        if use_after or use_before:
            token = str(c["after" if use_after else "before"])
            tk, td, tv = decode_keyset_v1(token)

            if tk != sort_keys or len(td) != len(directions):
                raise CoreError("Cursor does not match current search sort")

            for i, di in enumerate(directions):
                if (td[i] or "").lower() != di:
                    raise CoreError("Cursor does not match current search sort")

            sk, sp = build_seek_condition(
                exprs,
                directions,
                list(tv),
                "before" if use_before else "after",
            )

            where_fin = sk
            params = params + sp

        order_sql = self._hub_cursor_order_sql(
            exprs,
            sort_keys,
            directions,
            flip=use_before,
        )

        return_fields_sql: Sequence[str] | None

        if return_fields is not None:
            return_fields_sql = cursor_return_fields_for_select(
                sort_keys=sort_keys,
                rank_field=_RANK if do_legs else None,
                return_fields=return_fields,
            )
            if not return_fields_sql:
                return_fields_sql = None
        else:
            return_fields_sql = None

        base_cols = self.return_clause(
            return_type,
            return_fields_sql,
            table_alias=_COMBO_ALIAS,
        )

        cols: sql.Composable

        if do_legs:
            cols = sql.SQL("{}, {}").format(
                base_cols,
                sql.SQL("{} AS {}").format(
                    sql.SQL("{}.{}").format(
                        sql.Identifier(_COMBO_ALIAS),
                        sql.Identifier(_RANK),
                    ),
                    sql.Identifier(_RANK),
                ),
            )

        else:
            cols = base_cols

        data_stmt = sql.SQL(
            """
            {with_clause}
            SELECT {cols} FROM {combo} {ca}
            WHERE {w}
            ORDER BY {order}
            """
        ).format(
            with_clause=with_clause,
            cols=cols,
            combo=sql.Identifier("combo"),
            ca=sql.Identifier(_COMBO_ALIAS),
            w=where_fin,
            order=order_sql,
        )
        data_stmt = sql.SQL("{} LIMIT {}").format(
            data_stmt,
            sql.Placeholder(),
        )
        params.append(lim + 1)

        raw_rows = list(
            await self.client.fetch_all(data_stmt, params, row_factory="dict"),
        )  # type: ignore[assignment, arg-type]

        if use_before:
            raw_rows = list(reversed(raw_rows))

        has_more = len(raw_rows) > lim
        rows = raw_rows[:lim]

        def _row_token_vals(row: JsonDict) -> list[Any]:
            return [row_value_for_sort_key(row, k) for k in sort_keys]

        if has_more and rows:
            nxt = encode_keyset_v1(
                sort_keys=sort_keys,
                directions=directions,
                values=_row_token_vals(rows[-1]),
            )

        else:
            nxt = None

        if rows and (use_after or (use_before and has_more)):
            prv = encode_keyset_v1(
                sort_keys=sort_keys,
                directions=directions,
                values=_row_token_vals(rows[0]),
            )

        else:
            prv = None

        if return_type is not None:
            v = pydantic_validate_many(return_type, rows)

            return CursorPage(
                hits=v,
                next_cursor=nxt,
                prev_cursor=prv,
                has_more=has_more,
            )
        if return_fields is not None:
            rj = [{k: r.get(k, None) for k in return_fields} for r in rows]

            return CursorPage(
                hits=rj,
                next_cursor=nxt,
                prev_cursor=prv,
                has_more=has_more,
            )

        m = pydantic_validate_many(self.model_type, rows)

        return CursorPage(
            hits=m,
            next_cursor=nxt,
            prev_cursor=prv,
            has_more=has_more,
        )


# Backward-compatible alias (PGroonga-only name before engine pluggability).
PostgresHubPGroongaSearchAdapter = PostgresHubSearchAdapter
