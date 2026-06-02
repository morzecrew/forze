"""Postgres hub search adapter."""

from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from typing import Any, Literal, Mapping, Sequence, final

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
    normalize_search_queries,
    prepare_hub_search_options,
)
from forze.application.integrations.search import SearchResultSnapshot
from forze.base.exceptions import exc

from ....kernel.gateways import PostgresGateway
from .._offset_run import RankedOffsetPlan, execute_hub_ranked_offset_search
from .._pgroonga_plan import effective_combo_limit
from .._port import PostgresSearchPortMixin
from .._search_count import effective_search_count
from .constants import COMBO_ALIAS
from .cursor import HubSearchCursorMixin
from .parallel import HubParallelSearchMixin
from .runtime import HubLegRuntime

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresHubSearchAdapter[M: BaseModel](
    PostgresGateway[M],
    HubSearchCursorMixin[M],
    HubParallelSearchMixin[M],
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
        terms = normalize_search_queries(query)

        leg_options, member_weights_list = prepare_hub_search_options(
            self.hub_spec,
            options,
        )

        members_weighted: list[tuple[str, float]] = [
            (self.hub_spec.members[i].name, float(member_weights_list[i]))
            for i in range(len(self.hub_spec.members))
        ]

        active = [
            (i, leg)
            for i, leg in enumerate(self.members)
            if member_weights_list[i] > 0.0
        ]
        do_legs = bool(terms) and bool(active)
        use_parallel = (
            self.execution == "parallel"
            and do_legs
            and all(len(leg.hub_fk_columns) == 1 for _, leg in active)
        )

        if use_parallel:
            return await self._hub_parallel_offset_search(
                query=query,
                filters=filters,
                pagination=pagination,
                sorts=sorts,
                options=leg_options,
                snapshot=snapshot,
                return_count=return_count,
                return_type=return_type,
                return_fields=return_fields,
                hub_spec=self.hub_spec,
                members=self.members,
                vector_embedders=dict(self.vector_embedders),
                member_weights_list=member_weights_list,
                score_merge=self.score_merge,
                combine=self.combine,
                per_leg_limit=self.per_leg_limit,
                combo_limit_config=self.combo_limit,
                result_snapshot=self.result_snapshot,
            )

        rs_spec = self.hub_spec.snapshot
        resolved_combo = effective_combo_limit(
            config_limit=self.combo_limit,
            per_leg_limit=self.per_leg_limit,
            options=leg_options,
            pagination=dict(pagination or {}),
            snapshot=snapshot,
            result_snapshot=self.result_snapshot,
            rs_spec=rs_spec,
        )

        with_clause, params, do_legs, count_relation, data_relation = (
            await self._hub_build_with_clause(
                query_terms=terms,
                filters=filters,
                leg_options=leg_options,
                member_weights_list=member_weights_list,
                per_leg_limit=self.per_leg_limit,
                combo_limit=resolved_combo if do_legs else None,
            )
        )

        order_sql = await self._hub_order_sql_for_search(do_legs, sorts)

        approximate_total: int | None = None
        count_policy = effective_search_count(leg_options)

        if return_count and count_policy == "approximate":
            fw, fp = await self.where_clause(filters)
            hub_qn = await self._qname()
            approximate_total = await self.introspector.estimate_filtered_rows(
                schema=hub_qn.schema,
                relation=hub_qn.name,
                where_sql=fw,
                params=fp,
            )

        plan = RankedOffsetPlan(
            with_clause=with_clause,
            from_outer=sql.SQL(""),
            order_sql=order_sql,
            params=params,
            approximate_total=approximate_total,
            count_relation=count_relation,
            data_relation=data_relation,
            select_table_alias=COMBO_ALIAS,
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
            per_leg_limit=self.per_leg_limit,
            pagination=pagination,
            snapshot=snapshot,
            return_count=return_count,
            return_type=return_type,
            return_fields=return_fields,
            model_type=self.model_type,
            result_snapshot=self.result_snapshot,
            combo_alias=COMBO_ALIAS,
            options=leg_options,
        )
