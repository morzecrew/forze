"""Shared offset pagination + snapshot execution for ranked Postgres search."""

from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from typing import Any, Sequence, TypeVar

import attrs
from psycopg import sql
from pydantic import BaseModel

from forze.application.contracts.base import page_from_limit_offset
from forze.application.contracts.querying import (
    PaginationExpression,
    QueryFilterExpression,
    QuerySortExpression,
)
from forze.application.contracts.search import (
    HubSearchSpec,
    SearchOptions,
    SearchResultSnapshotOptions,
    SearchSpec,
    normalize_search_queries,
)
from forze.application.integrations.search import SearchResultSnapshot
from forze.application.integrations.search.offset_executor import (
    OffsetFetchWindow,
    OffsetRowsResult,
    execute_simple_offset_search_with_snapshot,
    offset_from_dict,
    snapshot_materialize_and_paginate,
)

from ...kernel.gateways import PostgresGateway
from ._search_count import effective_search_count

# ----------------------- #

M = TypeVar("M", bound=BaseModel)

# ....................... #


@attrs.define(frozen=True, slots=True, kw_only=True)
class RankedOffsetPlan:
    """SQL fragments for one ranked offset search (count + data)."""

    with_clause: sql.Composable
    """``WITH`` clause."""

    from_outer: sql.Composable
    """``FROM …`` fragment appended after ``SELECT cols``."""

    order_sql: sql.Composable
    """ORDER BY clause."""

    params: list[Any]
    """Parameters for the ``SELECT`` statement."""

    count_params: list[Any] | None = None
    """When set, used for ``COUNT(*)`` only (e.g. FTS empty-query uses filter params only)."""

    count_with_clause: sql.Composable | None = None
    """Uncapped ranked ``WITH`` for exact totals when data pipeline uses a candidate cap."""

    count_from_outer: sql.Composable | None = None

    approximate_total: int | None = None
    """When set, used for ``search_count=approximate`` instead of ``COUNT(*)``."""

    count_relation: str = "combo"
    """Hub - relation name for ``COUNT(*)`` (defaults to full ``combo``)."""

    data_relation: str = "combo"
    """Hub - relation name for the ranked data ``SELECT`` (e.g. ``combo_top``)."""

    select_table_alias: str
    """Table alias passed to :meth:`~PostgresGateway.return_clause`."""


# ....................... #


@attrs.define(slots=True)
class _PostgresSimpleOffsetHooks:
    gw: PostgresGateway[Any]
    plan: RankedOffsetPlan
    return_type: type[BaseModel] | None
    return_fields: Sequence[str] | None
    return_count: bool
    count_policy: str
    pagination_dict: dict[str, Any]

    async def fetch_count(self) -> int | None:
        if not self.return_count or self.count_policy == "none":
            return None

        if self.count_policy == "approximate" and self.plan.approximate_total is not None:
            return int(self.plan.approximate_total)

        use_uncapped_count = (
            self.count_policy == "exact"
            and self.plan.count_with_clause is not None
            and self.plan.count_from_outer is not None
        )

        if use_uncapped_count:
            count_with = self.plan.count_with_clause
            count_from = self.plan.count_from_outer
            count_params = (
                self.plan.count_params
                if self.plan.count_params is not None
                else self.plan.params
            )

        else:
            count_with = self.plan.with_clause
            count_from = self.plan.from_outer
            count_params = (
                self.plan.count_params
                if self.plan.count_params is not None
                else self.plan.params
            )

        count_stmt = sql.SQL(
            """
            {with_clause}
            SELECT COUNT(*) {from_outer}
            """
        ).format(with_clause=count_with, from_outer=count_from)

        return int(
            await self.gw.client.fetch_value(count_stmt, count_params, default=0),
        )

    async def fetch_rows(
        self,
        window: OffsetFetchWindow,
        *,
        want_snap: bool,
    ) -> OffsetRowsResult:
        cols = self.gw.return_clause(
            self.return_type,
            self.return_fields,
            table_alias=self.plan.select_table_alias,
        )

        data_stmt = sql.SQL(
            """
            {with_clause}
            SELECT {cols} {from_outer}
            ORDER BY {order}
            """
        ).format(
            with_clause=self.plan.with_clause,
            cols=cols,
            from_outer=self.plan.from_outer,
            order=self.plan.order_sql,
        )

        params = list(self.plan.params)

        if window.fetch_limit is not None:
            data_stmt += sql.SQL(" LIMIT {}").format(sql.Placeholder())
            params.append(int(window.fetch_limit))

        if want_snap:
            data_stmt += sql.SQL(" OFFSET {}").format(sql.Placeholder())
            params.append(int(window.fetch_offset))

        elif self.pagination_dict.get("offset") is not None:
            data_stmt += sql.SQL(" OFFSET {}").format(sql.Placeholder())
            params.append(offset_from_dict(self.pagination_dict))

        rows = await self.gw.client.fetch_all(data_stmt, params, row_factory="dict")

        return OffsetRowsResult(rows=list(rows))


# ....................... #


async def execute_simple_ranked_offset_search(
    gw: PostgresGateway[M],
    *,
    plan: RankedOffsetPlan,
    query: str | Sequence[str],
    filters: QueryFilterExpression | None,  # type: ignore[valid-type]
    sorts: QuerySortExpression | None,  # type: ignore[valid-type]
    spec: SearchSpec[Any],
    variant: str,
    fingerprint_extras: dict[str, object] | None,
    pagination: PaginationExpression | None,
    snapshot: SearchResultSnapshotOptions | None,
    return_count: bool,
    return_type: type[BaseModel] | None,
    return_fields: Sequence[str] | None,
    model_type: type[M],
    result_snapshot: SearchResultSnapshot | None,
    options: SearchOptions | None = None,
    trust_source: bool = False,
) -> Any:
    """Run count (optional), data fetch, snapshot materialization for simple search adapters."""

    count_policy = effective_search_count(options)
    pagination_dict: dict[str, Any] = dict(pagination or {})

    return await execute_simple_offset_search_with_snapshot(
        query=query,
        filters=filters,
        sorts=sorts,
        spec=spec,
        variant=variant,
        fingerprint_extras=fingerprint_extras,
        pagination=pagination,
        snapshot=snapshot,
        return_count=return_count,
        snapshot_return_count=return_count and count_policy != "none",
        page_return_count=return_count and count_policy != "none",
        return_type=return_type,
        return_fields=return_fields,
        model_type=model_type,
        codec=spec.resolved_read_codec,
        result_snapshot=result_snapshot,
        hooks=_PostgresSimpleOffsetHooks(
            gw=gw,
            plan=plan,
            return_type=return_type,
            return_fields=return_fields,
            return_count=return_count,
            count_policy=count_policy,
            pagination_dict=pagination_dict,
        ),
        trust_source=trust_source,
    )


# ....................... #


async def execute_hub_ranked_offset_search(
    gw: PostgresGateway[M],
    *,
    plan: RankedOffsetPlan,
    query: str | Sequence[str],
    filters: QueryFilterExpression | None,  # type: ignore[valid-type]
    sorts: QuerySortExpression | None,  # type: ignore[valid-type]
    hub_spec: HubSearchSpec[Any],
    members_weighted: list[tuple[str, float]],
    score_merge: str,
    combine: str,
    per_leg_limit: int,
    pagination: PaginationExpression | None,
    snapshot: SearchResultSnapshotOptions | None,
    return_count: bool,
    return_type: type[BaseModel] | None,
    return_fields: Sequence[str] | None,
    model_type: type[M],
    result_snapshot: SearchResultSnapshot | None,
    combo_alias: str = "comb",
    options: SearchOptions | None = None,
    execution: str | None = None,
    combo_limit: int | None = None,
    trust_source: bool = False,
) -> Any:
    """Ranked offset search for :class:`~forze_postgres.adapters.search.hub.PostgresHubSearchAdapter`."""

    rs_spec = hub_spec.snapshot
    count_policy = effective_search_count(options)
    snapshot_return_count = return_count and count_policy != "none"
    fp_fingerprint = SearchResultSnapshot.hub_search_fingerprint(
        query,
        filters,
        sorts,
        spec_name=hub_spec.name,
        members_weighted=members_weighted,
        score_merge=score_merge,
        combine=combine,
        per_leg_limit=per_leg_limit,
        execution=execution,
        combo_limit=combo_limit,
        search_count=count_policy,
    )

    if result_snapshot is not None and rs_spec is not None:
        read_page = await result_snapshot.read_hub_result_snapshot(
            rs_spec=rs_spec,
            snap_opt=snapshot,
            fp_computed=fp_fingerprint,
            model_type=model_type,
            pagination=dict(pagination or {}),
            return_type=return_type,
            return_fields=return_fields,
            return_count=snapshot_return_count,
        )

        if read_page is not None:
            return read_page

    count_stmt = sql.SQL(
        """
            {with_clause}
            SELECT COUNT(*) FROM {combo} {ca}
            """
    ).format(
        with_clause=plan.with_clause,
        combo=sql.Identifier(plan.count_relation),
        ca=sql.Identifier(combo_alias),
    )

    total = 0
    pagination_dict: dict[str, Any] = dict(pagination or {})

    if return_count and count_policy != "none":
        if count_policy == "approximate" and plan.approximate_total is not None:
            total = int(plan.approximate_total)

        elif count_policy == "exact" and hasattr(gw, "_hub_sql_combo_count"):
            hub_count = getattr(gw, "_hub_sql_combo_count")
            total = int(
                await hub_count(
                    query_terms=tuple(normalize_search_queries(query)),
                    filters=filters,
                    leg_options=options,
                    member_weights_list=[float(w) for _name, w in members_weighted],
                    per_leg_limit=per_leg_limit,
                    sorts=sorts if sorts else hub_spec.default_sort,
                )
            )

        else:
            total = int(await gw.client.fetch_value(count_stmt, plan.params, default=0))

        if return_count and total == 0:
            return page_from_limit_offset(  # pyright: ignore[reportUnknownVariableType]
                [],
                pagination or {},
                total=0,
            )

    cols = gw.return_clause(
        return_type,
        return_fields,
        table_alias=plan.select_table_alias,
    )

    data_stmt = sql.SQL(
        """
            {with_clause}
            SELECT {cols} FROM {combo} {ca}
            ORDER BY {order}
            """
    ).format(
        with_clause=plan.with_clause,
        cols=cols,
        combo=sql.Identifier(plan.data_relation),
        ca=sql.Identifier(combo_alias),
        order=plan.order_sql,
    )

    params = list(plan.params)

    want_sn = (
        result_snapshot is not None
        and rs_spec is not None
        and result_snapshot.should_write_result_snapshot(snapshot, rs_spec)
    )
    max_nh = (
        result_snapshot.effective_snapshot_max_ids(snapshot, rs_spec)
        if want_sn and result_snapshot is not None
        else 0
    )
    fetch_limit, fetch_offset, page_limit = SearchResultSnapshot.snapshot_pagination(
        want_sn, max_nh, pagination_dict
    )

    if fetch_limit is not None:
        data_stmt += sql.SQL(" LIMIT {}").format(sql.Placeholder())
        params.append(int(fetch_limit))

    if want_sn:
        data_stmt += sql.SQL(" OFFSET {}").format(sql.Placeholder())
        params.append(int(fetch_offset))

    elif pagination_dict.get("offset") is not None:
        data_stmt += sql.SQL(" OFFSET {}").format(sql.Placeholder())
        params.append(offset_from_dict(pagination_dict))

    rows = await gw.client.fetch_all(data_stmt, params, row_factory="dict")

    return await snapshot_materialize_and_paginate(
        rows=list(rows),
        want_snap=want_sn,
        result_snapshot=result_snapshot,
        rs_spec=rs_spec,
        snapshot=snapshot,
        fp_fingerprint=fp_fingerprint,
        pagination_dict=pagination_dict,
        page_limit=page_limit,
        return_count=return_count,
        total=total if count_policy != "none" else None,
        return_type=return_type,
        return_fields=return_fields,
        model_type=model_type,
        codec=hub_spec.resolved_read_codec,
        trust_source=trust_source,
    )
