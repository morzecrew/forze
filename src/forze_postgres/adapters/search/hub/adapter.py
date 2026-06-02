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
from .._port import PostgresSearchPortMixin
from .constants import COMBO_ALIAS
from .cursor import HubSearchCursorMixin
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

    # ....................... #

    def __attrs_post_init__(self) -> None:
        super().__attrs_post_init__()
        if self.per_leg_limit < 1:
            raise exc.internal("per_leg_limit must be at least 1.")

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
            per_leg_limit=self.per_leg_limit,
        )

        order_sql = await self._hub_order_sql_for_search(do_legs, sorts)

        plan = RankedOffsetPlan(
            with_clause=with_clause,
            from_outer=sql.SQL(""),
            order_sql=order_sql,
            params=params,
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
        )
