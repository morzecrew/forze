"""Hub leg runtime and per-engine leg builders."""

from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from typing import Any, Final, Literal, Mapping, Protocol, Sequence, final
from uuid import UUID

import attrs
from psycopg import sql

from forze.application.contracts.embeddings import EmbeddingsProviderPort
from forze.application.contracts.search import SearchOptions, SearchSpec
from forze.base.exceptions import exc
from forze_postgres.kernel.catalog.hub_fk_columns import normalize_hub_fk_columns
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.gateways import PostgresQualifiedName
from forze_postgres.kernel.relation import RelationSpec, resolve_postgres_qname

from .._fts_sql import FtsGroupLetter
from .._leg_fts import build_fts_leg
from .._leg_pgroonga import build_pgroonga_leg
from .._leg_vector import build_vector_leg
from .._vector_sql import VectorDistanceKind
from .constants import (
    HUB_CTE,
    LEG_EID,
    LEG_SCORE,
)

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class HubLegRuntime:
    """Resolved leg: :class:`SearchSpec` plus Postgres index/heap wiring."""

    search: SearchSpec[Any]
    """Search specification."""

    index_relation: RelationSpec
    """Index relation or tenant-scoped resolver."""

    index_heap_relation: RelationSpec
    """Heap relation the index is defined on or resolver."""

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

    async def resolve_index_qname(
        self,
        tenant_id: UUID | None,
    ) -> PostgresQualifiedName:
        return await resolve_postgres_qname(self.index_relation, tenant_id)

    # ....................... #

    async def resolve_index_heap_qname(
        self,
        tenant_id: UUID | None,
    ) -> PostgresQualifiedName:
        return await resolve_postgres_qname(self.index_heap_relation, tenant_id)

    # ....................... #

    def candidate_subquery(self, *, csub_alias: str) -> sql.Composable:
        """Distinct heap PK candidates from the hub CTE (UNION when multiple hub FKs)."""

        cols = self.hub_fk_columns
        hf = sql.Identifier(HUB_CTE)
        csub = sql.Identifier(csub_alias)

        if len(cols) == 1:
            fk = sql.Identifier(HUB_CTE, cols[0])
            return sql.SQL(
                "( SELECT DISTINCT {fk} AS cand_id FROM {hf} WHERE {fk} IS NOT NULL ) {csub}",
            ).format(fk=fk, hf=hf, csub=csub)

        branches = [
            sql.SQL(
                "( SELECT DISTINCT {fk} AS cand_id FROM {hf} WHERE {fk} IS NOT NULL )",
            ).format(fk=sql.Identifier(HUB_CTE, col), hf=hf)
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
        t_eid = sql.SQL("{}.{}").format(t, sql.Identifier(LEG_EID))
        t_s = sql.SQL("{}.{}").format(t, sql.Identifier(LEG_SCORE))
        hf_fk = sql.Identifier(HUB_CTE, col)

        return sql.SQL(
            "LEFT JOIN ( "
            "SELECT DISTINCT ON ({t_eid}) {t_eid} AS {eid}, {t_s} AS {sc} "
            "FROM {lr} {t} "
            "ORDER BY {t_eid}, {t_s} DESC NULLS LAST"
            ") {pick} ON ({hf_fk} = {t_eid_qualified})"
        ).format(
            t_eid=t_eid,
            eid=sql.Identifier(LEG_EID),
            t_s=t_s,
            sc=sql.Identifier(LEG_SCORE),
            lr=lr,
            t=t,
            pick=pick,
            hf_fk=hf_fk,
            t_eid_qualified=sql.SQL("{}.{}").format(
                pick,
                sql.Identifier(LEG_EID),
            ),
        )

    # ....................... #

    @staticmethod
    def leg_u_cte(*, leg_cte_alias: str, u_cte_name: str) -> sql.Composable:
        """Deduplicate a leg to one ``(eid, s)`` per ``eid`` (best ``s``), for multi-FK joins."""

        lr = sql.Identifier(leg_cte_alias)
        lr_u = sql.Identifier(u_cte_name)
        t = sql.Identifier("t")
        t_eid = sql.SQL("{}.{}").format(t, sql.Identifier(LEG_EID))
        t_s = sql.SQL("{}.{}").format(t, sql.Identifier(LEG_SCORE))

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
            eid=sql.Identifier(LEG_EID),
            sc=sql.Identifier(LEG_SCORE),
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
            hf_fk = sql.Identifier(HUB_CTE, col)
            t_eid_q = sql.SQL("{}.{}").format(pick, sql.Identifier(LEG_EID))

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

        from .semantics import sql_leg_coalesce

        return sql_leg_coalesce(self, leg_index)

    # ....................... #

    def merge_matched(self, leg_index: int) -> sql.Composable:
        """Whether this leg matched: non-null leg ``eid`` on any FK join branch."""

        from .semantics import sql_leg_matched

        return sql_leg_matched(self, leg_index)


# ....................... #


class HubSearchLegEngine(Protocol):
    """Builds heap ``WHERE``, rank column, and parameters for one hub leg."""

    async def build_leg(
        self,
        leg: HubLegRuntime,
        *,
        tenant_id: UUID | None,
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
        tenant_id: UUID | None,
        introspector: PostgresIntrospector,
        index_alias: str,
        queries: tuple[str, ...],
        options: SearchOptions | None,
        score_column: str,
    ) -> tuple[sql.Composable, sql.Composable, list[Any]]:
        return await build_pgroonga_leg(
            introspector=introspector,
            index_qname=await leg.resolve_index_qname(tenant_id),
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
        tenant_id: UUID | None,
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
            index_qname=await leg.resolve_index_qname(tenant_id),
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
        tenant_id: UUID | None,
        introspector: PostgresIntrospector,
        index_alias: str,
        queries: tuple[str, ...],
        options: SearchOptions | None,
        score_column: str,
    ) -> tuple[sql.Composable, sql.Composable, list[Any]]:
        _ = tenant_id

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
