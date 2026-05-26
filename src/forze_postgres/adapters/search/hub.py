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
)
from forze.application.contracts.embeddings import EmbeddingsProviderPort
from forze.application.contracts.querying import (
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
    SearchSpec,
    cursor_return_fields_for_select,
    normalize_search_queries,
    prepare_hub_search_options,
)
from forze.application.coordinators import SearchResultSnapshotCoordinator
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict
from forze.base.serialization import pydantic_validate_many
from forze.domain.constants import ID_FIELD

from ...kernel.gateways import PostgresGateway, PostgresQualifiedName
from ...kernel.hub_fk_columns import normalize_hub_fk_columns
from ...kernel.introspect import PostgresIntrospector
from ...kernel.query.nested import sort_key_expr
from ...pagination import build_seek_condition
from ._fts_sql import FtsGroupLetter
from ._leg_fts import build_fts_leg
from ._leg_pgroonga import build_pgroonga_leg
from ._leg_vector import build_vector_leg
from ._offset_run import RankedOffsetPlan, execute_hub_ranked_offset_search
from ._vector_sql import VectorDistanceKind

# ----------------------- #

T = TypeVar("T", bound=BaseModel)

# ....................... #

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


@attrs.define(slots=True, kw_only=True, frozen=True)
class HubLegRuntime:
    """Resolved leg: :class:`SearchSpec` plus Postgres index/heap wiring."""

    search: SearchSpec[Any]
    """Search specification."""

    index_qname: PostgresQualifiedName
    """Qualified name for configuration symmetry (index object); not read at query time."""

    index_heap_qname: PostgresQualifiedName
    """Heap that holds the ``vector`` column used for distance scoring."""

    hub_fk_columns: tuple[str, ...] = attrs.field(converter=normalize_hub_fk_columns)
    """Foreign key columns used to join the leg to the hub."""

    heap_pk_column: str
    """Primary key column of the heap."""

    index_field_map: Mapping[str, str] | None = attrs.field(default=None)
    """Optional map from :class:`SearchSpec` field names to heap column names (unused in v2)."""

    pgroonga_score_version: Literal["v1", "v2"] | None = attrs.field(default=None)
    """``v1`` / ``v2`` :func:`pgroonga_score` form when :attr:`engine` is ``pgroonga``; else ``None``."""

    engine: Literal["pgroonga", "fts", "vector"] = "pgroonga"
    """Engine for the hub leg."""

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

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.engine == "vector":
            if not self.vector_column or self.embedding_dimensions is None:
                raise exc.internal(
                    "Vector hub leg requires vector_column and embedding_dimensions.",
                )

    # ....................... #

    def candidate_subquery(self, *, csub_alias: str) -> sql.Composable:
        """Distinct heap PK candidates from the hub CTE (UNION when multiple hub FKs)."""

        cols = self.hub_fk_columns
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

    # ....................... #

    def equi_pick_join(self, *, leg_cte_alias: str, pick_alias: str) -> sql.Composable:
        """Equi-join the single hub FK to the leg (``eid``, ``s``).

        Inlines ``LEFT JOIN (SELECT DISTINCT ON (eid) …) … ON (hf.fk = eid)`` so the
        planner can use a hash/merge plan. For multiple hub FKs, use
        :func:`_hub_leg_leg_u_cte` and :func:`_hub_leg_multi_equi_pick_join`.
        """

        (col,) = self.hub_fk_columns

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

    # ....................... #

    @staticmethod
    def leg_u_cte(*, leg_cte_alias: str, u_cte_name: str) -> sql.Composable:
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

    # ....................... #

    def multi_equi_pick_join(
        self,
        leg_u_cte: str,
        base_pick_prefix: str,
    ) -> sql.Composable:
        """K ``LEFT JOIN``s from hub FK columns to deduplicated ``leg_u`` (OR + best score in SELECT)."""

        leg_u = sql.Identifier(leg_u_cte)
        parts: list[sql.Composable] = []

        for j, col in enumerate(self.hub_fk_columns):
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

    # ....................... #

    def merge_coalesce(self, leg_index: int) -> sql.Composable:
        """Per-leg match score: single FK uses one join; multi-FK uses ``GREATEST`` of K joins."""

        if len(self.hub_fk_columns) == 1:
            return sql.SQL("COALESCE({}.{}, 0)").format(
                sql.Identifier(f"lp{leg_index}"),
                sql.Identifier(_LEG_SCORE),
            )

        br = [
            sql.SQL("COALESCE({}.{}, 0)").format(
                sql.Identifier(f"lp{leg_index}_{j}"),
                sql.Identifier(_LEG_SCORE),
            )
            for j in range(len(self.hub_fk_columns))
        ]

        return sql.SQL("GREATEST({})").format(sql.SQL(", ").join(br))

    # ....................... #

    def merge_matched(self, leg_index: int) -> sql.Composable:
        """Whether this leg matched: non-null leg ``eid`` on any FK join branch."""

        if len(self.hub_fk_columns) == 1:
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
            for j in range(len(self.hub_fk_columns))
        ]
        return sql.SQL("({})").format(sql.SQL(" OR ").join(eid_null))


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
        return await build_pgroonga_leg(
            introspector=introspector,
            index_qname=leg.index_qname,
            search=leg.search,
            index_field_map=leg.index_field_map,
            index_alias=index_alias,
            queries=queries,
            options=options,
            score_column=score_column,
            pgroonga_score_version=leg.pgroonga_score_version or "v2",
        )


# ....................... #


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
        groups = leg.fts_groups

        if groups is None:
            raise exc.internal("FTS hub leg requires fts_groups.")

        return await build_fts_leg(
            introspector=introspector,
            index_qname=leg.index_qname,
            search=leg.search,
            fts_groups=groups,
            index_alias=index_alias,
            queries=queries,
            options=options,
            score_column=score_column,
        )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class VectorHubLegEngine(HubSearchLegEngine):
    """Vector hub legs: KNN score on a ``vector`` heap column."""

    embedder: EmbeddingsProviderPort
    """Embedder for vector queries."""

    # ....................... #

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
        if leg.engine != "vector" or leg.vector_column is None:
            raise exc.internal("VectorHubLegEngine requires a vector hub leg.")

        edim = leg.embedding_dimensions

        if edim is None:
            raise exc.internal("embedding_dimensions is required for vector engine.")

        return await build_vector_leg(
            embedder=self.embedder,
            introspector=introspector,
            index_alias=index_alias,
            vector_column=leg.vector_column,
            vector_distance=leg.vector_distance,
            embedding_dimensions=edim,
            queries=queries,
            options=options,
            score_column=score_column,
        )


# ....................... #

_PGROONGA_HUB_LEG_ENGINE: Final[PgroongaHubLegEngine] = PgroongaHubLegEngine()
_FTS_HUB_LEG_ENGINE: Final[FtsHubLegEngine] = FtsHubLegEngine()

# ....................... #


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
            raise exc.internal("Vector hub leg requires an embeddings provider.")

        return VectorHubLegEngine(embedder=vector_embedder)

    raise exc.internal(f"Unsupported hub search leg engine: {eng!r}.")


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresHubSearchAdapter[M: BaseModel](
    PostgresGateway[M],
    SearchQueryPort[M],
):
    """Search over a hub row type with one or more legs and merged per-leg scores.

    Each leg's :attr:`~HubLegRuntime.engine` selects the implementation
    (PGroonga, FTS, or :class:`VectorHubLegEngine`). Built via
    :class:`ConfigurablePostgresHubSearch` from :class:`PostgresHubSearchConfig`.
    """

    hub_spec: HubSearchSpec[M]
    members: Sequence[HubLegRuntime]
    vector_embedders: Mapping[int, EmbeddingsProviderPort] = attrs.field(
        factory=dict[int, EmbeddingsProviderPort],
    )
    """Per-leg index → embedder for :attr:`~HubLegRuntime.engine` ``vector`` legs."""

    snapshot_coord: SearchResultSnapshotCoordinator | None = None
    """Coordinator for KV ordered-ID snapshots."""

    combine: Literal["or", "and"] = "or"
    """Combine mode for leg scores."""

    score_merge: Literal["max", "sum"] = "max"
    """Score merge mode for leg scores."""

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

    # ....................... #

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
                    cand_sub = leg.candidate_subquery(csub_alias=f"csub{i}")
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
                        leg.leg_u_cte(
                            leg_cte_alias=lr_alias,
                            u_cte_name=f"{lr_alias}_u",
                        ),
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
                    leg.merge_coalesce(i),
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
                        leg.equi_pick_join(
                            leg_cte_alias=leg_aliases[i],
                            pick_alias=f"lp{i}",
                        ),
                    )

                else:
                    join_parts.append(
                        leg.multi_equi_pick_join(
                            leg_u_cte=f"{leg_aliases[i]}_u",
                            base_pick_prefix=f"lp{i}",
                        ),
                    )

            leg_joins = sql.SQL(" ").join(join_parts)
            leg_null_checks = [leg.merge_matched(i) for i, leg, _ in active]

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

    # ....................... #

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
                    raise exc.internal(
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

    # ....................... #

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
    async def _offset_search_impl(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        snapshot: SearchResultSnapshotOptions | None = None,
        return_count: Literal[False],
        return_type: None = None,
        return_fields: None = None,
    ) -> CountlessPage[M]: ...

    @overload
    async def _offset_search_impl(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        snapshot: SearchResultSnapshotOptions | None = None,
        return_count: Literal[True],
        return_type: None = None,
        return_fields: None = None,
    ) -> Page[M]: ...

    @overload
    async def _offset_search_impl(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        snapshot: SearchResultSnapshotOptions | None = None,
        return_count: Literal[False],
        return_type: None = None,
        return_fields: Sequence[str],
    ) -> CountlessPage[JsonDict]: ...

    @overload
    async def _offset_search_impl(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        snapshot: SearchResultSnapshotOptions | None = None,
        return_count: Literal[True],
        return_type: None = None,
        return_fields: Sequence[str],
    ) -> Page[JsonDict]: ...

    @overload
    async def _offset_search_impl(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        snapshot: SearchResultSnapshotOptions | None = None,
        return_count: Literal[False],
        return_type: type[T],
        return_fields: None = None,
    ) -> CountlessPage[T]: ...

    @overload
    async def _offset_search_impl(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        snapshot: SearchResultSnapshotOptions | None = None,
        return_count: Literal[True],
        return_type: type[T],
        return_fields: None = None,
    ) -> Page[T]: ...

    async def _offset_search_impl(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        snapshot: SearchResultSnapshotOptions | None = None,
        return_count: bool,
        return_type: type[BaseModel] | None = None,
        return_fields: Sequence[str] | None = None,
    ) -> Any:
        terms = normalize_search_queries(query)

        leg_options, member_weights_list = prepare_hub_search_options(
            self.hub_spec,
            options,
        )

        members_weighted: list[tuple[str, float]] = [
            (self.hub_spec.members[i].name, float(member_weights_list[i]))
            for i in range(len(self.hub_spec.members))
        ]

        with_clause, params, do_legs = await self._hub_build_with_clause(
            query_terms=terms,
            filters=filters,
            leg_options=leg_options,
            member_weights_list=member_weights_list,
        )

        order_sql = await self._hub_order_sql_for_search(do_legs, sorts)

        plan = RankedOffsetPlan(
            with_clause=with_clause,
            from_outer=sql.SQL(""),
            order_sql=order_sql,
            params=params,
            select_table_alias=_COMBO_ALIAS,
        )

        return await execute_hub_ranked_offset_search(
            self,
            plan=plan,
            query=query,
            filters=filters,
            sorts=sorts,
            hub_spec=self.hub_spec,
            members_weighted=members_weighted,
            score_merge=str(self.score_merge),
            combine=str(self.combine),
            pagination=pagination,
            snapshot=snapshot,
            return_count=return_count,
            return_type=return_type,
            return_fields=return_fields,
            model_type=self.model_type,
            snapshot_coord=self.snapshot_coord,
            combo_alias=_COMBO_ALIAS,
        )

    # ....................... #

    async def search(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        snapshot: SearchResultSnapshotOptions | None = None,
    ) -> CountlessPage[M]:
        return await self._offset_search_impl(
            query,
            filters,
            pagination,
            sorts,
            options=options,
            snapshot=snapshot,
            return_count=False,
            return_type=None,
            return_fields=None,
        )

    # ....................... #

    async def search_page(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        snapshot: SearchResultSnapshotOptions | None = None,
    ) -> Page[M]:
        return await self._offset_search_impl(
            query,
            filters,
            pagination,
            sorts,
            options=options,
            snapshot=snapshot,
            return_count=True,
            return_type=None,
            return_fields=None,
        )

    # ....................... #

    async def project_search(
        self,
        fields: Sequence[str],
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        snapshot: SearchResultSnapshotOptions | None = None,
    ) -> CountlessPage[JsonDict]:
        return await self._offset_search_impl(
            query,
            filters,
            pagination,
            sorts,
            options=options,
            snapshot=snapshot,
            return_count=False,
            return_type=None,
            return_fields=tuple(fields),
        )

    # ....................... #

    async def project_search_page(
        self,
        fields: Sequence[str],
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        snapshot: SearchResultSnapshotOptions | None = None,
    ) -> Page[JsonDict]:
        return await self._offset_search_impl(
            query,
            filters,
            pagination,
            sorts,
            options=options,
            snapshot=snapshot,
            return_count=True,
            return_type=None,
            return_fields=tuple(fields),
        )

    # ....................... #

    async def select_search(
        self,
        return_type: type[T],
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        snapshot: SearchResultSnapshotOptions | None = None,
    ) -> CountlessPage[T]:
        return await self._offset_search_impl(
            query,
            filters,
            pagination,
            sorts,
            options=options,
            snapshot=snapshot,
            return_count=False,
            return_type=return_type,
            return_fields=None,
        )

    # ....................... #

    async def select_search_page(
        self,
        return_type: type[T],
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        snapshot: SearchResultSnapshotOptions | None = None,
    ) -> Page[T]:
        return await self._offset_search_impl(
            query,
            filters,
            pagination,
            sorts,
            options=options,
            snapshot=snapshot,
            return_count=True,
            return_type=return_type,
            return_fields=None,
        )

    # ....................... #

    @overload
    async def _cursor_search_impl(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        return_type: None = None,
        return_fields: None = None,
    ) -> CursorPage[M]: ...

    @overload
    async def _cursor_search_impl(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        return_type: None = None,
        return_fields: Sequence[str],
    ) -> CursorPage[JsonDict]: ...

    @overload
    async def _cursor_search_impl(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        return_type: type[T],
        return_fields: None = None,
    ) -> CursorPage[T]: ...

    async def _cursor_search_impl(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        return_type: type[BaseModel] | None = None,
        return_fields: Sequence[str] | None = None,
    ) -> Any:
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
            raise exc.internal(
                "Cursor pagination: pass at most one of 'after' or 'before'",
            )

        lim: int = 10 if c.get("limit") is None else int(c["limit"])  # type: ignore[arg-type, assignment, call-overload]

        if lim < 1:
            raise exc.internal("Cursor pagination 'limit' must be positive")

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
                raise exc.internal("Cursor does not match current search sort")

            for i, di in enumerate(directions):
                if (td[i] or "").lower() != di:
                    raise exc.internal("Cursor does not match current search sort")

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

    # ....................... #

    async def search_cursor(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
    ) -> CursorPage[M]:
        return await self._cursor_search_impl(
            query,
            filters,
            cursor,
            sorts,
            options=options,
            return_type=None,
            return_fields=None,
        )

    # ....................... #

    async def project_search_cursor(
        self,
        fields: Sequence[str],
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
    ) -> CursorPage[JsonDict]:
        return await self._cursor_search_impl(
            query,
            filters,
            cursor,
            sorts,
            options=options,
            return_type=None,
            return_fields=tuple(fields),
        )

    # ....................... #

    async def select_search_cursor(
        self,
        return_type: type[T],
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
    ) -> CursorPage[T]:
        return await self._cursor_search_impl(
            query,
            filters,
            cursor,
            sorts,
            options=options,
            return_type=return_type,
            return_fields=None,
        )
