"""Postgres hub search adapter."""

from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from typing import Any, Literal, Mapping, Sequence, cast, final

import attrs
from psycopg import sql
from pydantic import BaseModel

from forze.application.contracts.embeddings import EmbeddingsProviderPort
from forze.application.contracts.querying import (
    PaginationExpression,
    QueryFilterExpression,
    QuerySortExpression,
)
from forze.application.contracts.search import (
    HubSearchSpec,
    SearchOptions,
    SearchQueryPort,
    SearchResultSnapshotOptions,
    reject_unsupported_facets,
    reject_unsupported_highlight,
)
from forze.application.integrations.search import SearchResultSnapshot
from forze.base.exceptions import exc

from ....kernel.gateways import PostgresGateway
from .._materialize_hits import search_trust_source
from .._offset_run import RankedOffsetPlan, execute_hub_ranked_offset_search
from .._port import PostgresSearchPortMixin
from .._search_count import resolve_ranked_approximate_total
from ._typing_host import HubSearchHost
from .constants import COMBO_ALIAS
from .cursor import HubSearchCursorMixin
from .plan import build_hub_search_plan, hub_members_weighted
from .runtime import HubLegRuntime

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresHubSearchAdapter[M: BaseModel](
    PostgresGateway[M],
    HubSearchCursorMixin[M],
    PostgresSearchPortMixin[M],
    SearchQueryPort[M],
):
    """Search over a hub row type with one or more legs and merged per-leg scores."""

    hub_spec: HubSearchSpec[M]
    members: Sequence[HubLegRuntime]
    vector_embedders: Mapping[int, EmbeddingsProviderPort] = attrs.field(
        factory=dict[int, EmbeddingsProviderPort],
    )
    result_snapshot: SearchResultSnapshot | None = None
    combine: Literal["or", "and"] = "or"
    score_merge: Literal["max", "sum"] = "max"
    per_leg_limit: int = 5000
    """Max ranked rows retained per hub leg before merge."""

    combo_limit: int | None = None
    """Cap merged hub rows before outer pagination; ``None`` derives from :attr:`per_leg_limit`."""

    execution: Literal["sql", "parallel"] = "sql"
    """``sql``: one ``WITH`` query; ``parallel``: per-leg queries merged in Python."""

    parallel_hub_cte_materialized: bool = True
    """When ``execution=parallel``, use ``MATERIALIZED`` on the hub filter CTE per leg statement."""

    read_validation: Literal["strict", "trusted"] = "strict"
    """Row decode mode for hub search hits (``trusted`` skips Pydantic validation)."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        super().__attrs_post_init__()

        if self.per_leg_limit < 1:
            raise exc.internal("per_leg_limit must be at least 1.")

        if self.combo_limit is not None and self.combo_limit < 1:
            raise exc.internal("combo_limit must be at least 1.")

        if self.execution not in ("sql", "parallel"):
            raise exc.internal("execution must be 'sql' or 'parallel'.")

    # ....................... #

    async def _offset_search_impl(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        snapshot: SearchResultSnapshotOptions | None = None,
        return_count: bool = False,
        return_type: type[BaseModel] | None = None,
        return_fields: Sequence[str] | None = None,
    ) -> Any:
        # Postgres hub search merges heterogeneous leg engines into one combined SQL, so a
        # single facet companion query and a single highlight engine over the merged row are
        # ill-defined; fail closed rather than silently drop (mock hub is the reference shape).
        reject_unsupported_facets(options, backend="Postgres hub")
        reject_unsupported_highlight(self.hub_spec, options, backend="Postgres hub")

        plan = await build_hub_search_plan(
            cast(HubSearchHost[Any], self),
            query=query,
            options=options,
            sorts=sorts,
            pagination_or_cursor=dict(pagination or {}),
            snapshot=snapshot,
            result_snapshot=self.result_snapshot,
            mode="offset",
        )

        if plan.use_parallel:
            return await self._hub_parallel_offset_search(
                plan=plan,
                query=query,
                filters=filters,
                pagination=pagination,
                sorts=sorts,
                options=plan.leg_options,
                snapshot=snapshot,
                return_count=return_count,
                return_type=return_type,
                return_fields=return_fields,
                hub_spec=self.hub_spec,
                result_snapshot=self.result_snapshot,
            )

        combo_cap = plan.resolved_combo if plan.do_legs else None

        # Late materialization: project only key/sort columns through the WITH pipeline and
        # hydrate the heavy read-model columns for the final page by id. Disabled when this
        # request writes a result snapshot (that path streams the whole result window and
        # needs the full projection) or when the shape can't be thinned safely.
        rs_spec = self.hub_spec.snapshot
        writes_snapshot = (
            self.result_snapshot is not None
            and rs_spec is not None
            and SearchResultSnapshot.should_write_result_snapshot(snapshot, rs_spec)
        )
        thin = (not writes_snapshot) and self._hub_thin_projection(plan) is not None

        # do_legs (_) is not used for some reason
        with_clause, params, _, count_relation, data_relation = (
            await self._hub_build_with_clause_from_plan(
                plan,
                filters=filters,
                combo_limit=combo_cap,
                thin=thin,
            )
        )

        order_sql = await self.render_hub_order_sql(plan)

        approximate_total: int | None = None

        if return_count and plan.count_policy == "approximate":
            fw, fp = await self.where_clause(filters)
            hub_qn = await self._qname()
            approximate_total = await resolve_ranked_approximate_total(
                introspector=self.introspector,
                schema=hub_qn.schema,
                relation=hub_qn.name,
                where_sql=fw,
                params=fp,
                combo_limit=combo_cap,
            )

        ranked_plan = RankedOffsetPlan(
            with_clause=with_clause,
            from_outer=sql.SQL(""),
            order_sql=order_sql,
            params=params,
            approximate_total=approximate_total,
            count_relation=count_relation,
            data_relation=data_relation,
            thin=thin,
            select_table_alias=COMBO_ALIAS,
        )

        members_weighted = hub_members_weighted(
            self.hub_spec,
            plan.member_weights_list,
        )

        return await execute_hub_ranked_offset_search(
            self,
            plan=ranked_plan,
            query=query,
            filters=filters,
            sorts=sorts,
            hub_spec=self.hub_spec,
            members_weighted=members_weighted,
            score_merge=str(self.score_merge),
            combine=str(self.combine),
            per_leg_limit=self.per_leg_limit,
            pagination=pagination,
            snapshot=snapshot,
            return_count=return_count,
            return_type=return_type,
            return_fields=return_fields,
            model_type=self.model_type,
            result_snapshot=self.result_snapshot,
            combo_alias=COMBO_ALIAS,
            options=plan.leg_options,
            execution=str(self.execution),
            combo_limit=combo_cap,
            trust_source=search_trust_source(self.read_validation),
        )
