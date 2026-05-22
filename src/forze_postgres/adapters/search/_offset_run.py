"""Shared offset pagination + snapshot execution for ranked Postgres search."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, TypeVar

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
    SearchResultSnapshotOptions,
    SearchSpec,
)
from forze.application.coordinators import SearchResultSnapshotCoordinator
from forze.base.serialization import pydantic_validate_many

from ...kernel.gateways import PostgresGateway
from ._materialize_hits import materialize_search_page

# ----------------------- #

M = TypeVar("M", bound=BaseModel)


@attrs.define(frozen=True, slots=True, kw_only=True)
class RankedOffsetPlan:
    """SQL fragments for one ranked offset search (count + data)."""

    with_clause: sql.Composable
    from_outer: sql.Composable
    """``FROM …`` fragment appended after ``SELECT cols``."""
    order_sql: sql.Composable
    params: list[Any]
    count_params: list[Any] | None = None
    """When set, used for ``COUNT(*)`` only (e.g. FTS empty-query uses filter params only)."""
    select_table_alias: str
    """Table alias passed to :meth:`~PostgresGateway.return_clause`."""


# ....................... #


def _offset_from_dict(pagination_dict: dict[str, Any]) -> int:
    """Read ``offset`` from a pagination mapping (``dict(pagination)``)."""

    raw = pagination_dict.get("offset")

    if raw is None:
        return 0

    return int(raw)


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
    snapshot_coord: SearchResultSnapshotCoordinator | None,
) -> Any:
    """Run count (optional), data fetch, snapshot materialization for simple search adapters."""

    rs_spec = spec.snapshot
    fp_fingerprint = SearchResultSnapshotCoordinator.simple_search_fingerprint(
        query,
        filters,
        sorts,
        spec_name=spec.name,
        variant=variant,
        extras=fingerprint_extras,
    )

    if snapshot_coord is not None and rs_spec is not None:
        maybe_snap: Any = await snapshot_coord.read_simple_result_snapshot(
            rs_spec=rs_spec,
            snap_opt=snapshot,
            fp_computed=fp_fingerprint,
            spec=spec,
            pagination=dict(pagination or {}),
            return_type=return_type,
            return_fields=return_fields,
            return_count=return_count,
        )

        if maybe_snap is not None:
            return maybe_snap

    count_params = plan.count_params if plan.count_params is not None else plan.params

    count_stmt = sql.SQL(
        """
            {with_clause}
            SELECT COUNT(*) {from_outer}
            """
    ).format(with_clause=plan.with_clause, from_outer=plan.from_outer)

    total = 0

    if return_count:
        total = int(
            await gw.client.fetch_value(count_stmt, count_params, default=0),
        )

        if total == 0:
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
            SELECT {cols} {from_outer}
            ORDER BY {order}
            """
    ).format(
        with_clause=plan.with_clause,
        cols=cols,
        from_outer=plan.from_outer,
        order=plan.order_sql,
    )

    params = list(plan.params)
    pagination_dict: dict[str, Any] = dict(pagination or {})

    want_snap = (
        snapshot_coord is not None
        and rs_spec is not None
        and snapshot_coord.should_write_result_snapshot(snapshot, rs_spec)
    )
    max_nw = (
        snapshot_coord.effective_snapshot_max_ids(snapshot, rs_spec)
        if want_snap and snapshot_coord is not None
        else 0
    )
    sql_limit, sql_offset, page_limit = (
        SearchResultSnapshotCoordinator.snapshot_pagination(
            want_snap, max_nw, pagination_dict
        )
    )

    if sql_limit is not None:
        data_stmt += sql.SQL(" LIMIT {}").format(sql.Placeholder())
        params.append(int(sql_limit))

    if want_snap:
        data_stmt += sql.SQL(" OFFSET {}").format(sql.Placeholder())
        params.append(int(sql_offset))

    elif pagination_dict.get("offset") is not None:
        data_stmt += sql.SQL(" OFFSET {}").format(sql.Placeholder())
        params.append(_offset_from_dict(pagination_dict))

    rows = await gw.client.fetch_all(data_stmt, params, row_factory="dict")

    handle_out = None
    pool_snap: list[M] | None = None
    u_off = _offset_from_dict(pagination_dict)

    if want_snap and snapshot_coord is not None and rs_spec is not None:
        pool_len = len(rows)
        pool_snap = pydantic_validate_many(model_type, rows)
        handle_out = await snapshot_coord.put_simple_ordered_hits(
            pool_snap,
            snap_opt=snapshot,
            rs_spec=rs_spec,
            fp_computed=fp_fingerprint,
            pool_len_before_cap=pool_len,
        )
        rows = rows[u_off : u_off + page_limit]

    page = materialize_search_page(
        page_rows=rows,
        pool=pool_snap,
        u=u_off,
        page_limit=page_limit,
        return_type=return_type,
        return_fields=return_fields,
        model_type=model_type,
    )

    if return_count:
        return page_from_limit_offset(
            page,
            pagination_dict,
            total=total,
            snapshot=handle_out,
        )

    return page_from_limit_offset(
        page,
        pagination_dict,
        total=None,
        snapshot=handle_out,
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
    pagination: PaginationExpression | None,
    snapshot: SearchResultSnapshotOptions | None,
    return_count: bool,
    return_type: type[BaseModel] | None,
    return_fields: Sequence[str] | None,
    model_type: type[M],
    snapshot_coord: SearchResultSnapshotCoordinator | None,
    combo_alias: str = "comb",
) -> Any:
    """Ranked offset search for :class:`~forze_postgres.adapters.search.hub.PostgresHubSearchAdapter`."""

    rs_spec = hub_spec.snapshot
    fp_fingerprint = SearchResultSnapshotCoordinator.hub_search_fingerprint(
        query,
        filters,
        sorts,
        spec_name=hub_spec.name,
        members_weighted=members_weighted,
        score_merge=score_merge,
        combine=combine,
    )

    if snapshot_coord is not None and rs_spec is not None:
        read_page = await snapshot_coord.read_hub_result_snapshot(
            rs_spec=rs_spec,
            snap_opt=snapshot,
            fp_computed=fp_fingerprint,
            model_type=model_type,
            pagination=dict(pagination or {}),
            return_type=return_type,
            return_fields=return_fields,
            return_count=return_count,
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
        combo=sql.Identifier("combo"),
        ca=sql.Identifier(combo_alias),
    )

    total = 0

    if return_count:
        total = int(await gw.client.fetch_value(count_stmt, plan.params, default=0))

        if total == 0:
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
        combo=sql.Identifier("combo"),
        ca=sql.Identifier(combo_alias),
        order=plan.order_sql,
    )

    params = list(plan.params)
    pagination_dict: dict[str, Any] = dict(pagination or {})

    want_sn = (
        snapshot_coord is not None
        and rs_spec is not None
        and snapshot_coord.should_write_result_snapshot(snapshot, rs_spec)
    )
    max_nh = (
        snapshot_coord.effective_snapshot_max_ids(snapshot, rs_spec)
        if want_sn and snapshot_coord is not None
        else 0
    )
    sql_limit, sql_offset, page_limit = (
        SearchResultSnapshotCoordinator.snapshot_pagination(
            want_sn, max_nh, pagination_dict
        )
    )

    if sql_limit is not None:
        data_stmt += sql.SQL(" LIMIT {}").format(sql.Placeholder())
        params.append(int(sql_limit))

    if want_sn:
        data_stmt += sql.SQL(" OFFSET {}").format(sql.Placeholder())
        params.append(int(sql_offset))

    elif pagination_dict.get("offset") is not None:
        data_stmt += sql.SQL(" OFFSET {}").format(sql.Placeholder())
        params.append(_offset_from_dict(pagination_dict))

    rows = await gw.client.fetch_all(data_stmt, params, row_factory="dict")

    handle_h = None
    pool_h: list[M] | None = None
    u_h = _offset_from_dict(pagination_dict)

    if want_sn and snapshot_coord is not None and rs_spec is not None:
        plh = len(rows)
        pool_h = pydantic_validate_many(model_type, rows)
        handle_h = await snapshot_coord.put_simple_ordered_hits(
            pool_h,
            snap_opt=snapshot,
            rs_spec=rs_spec,
            fp_computed=fp_fingerprint,
            pool_len_before_cap=plh,
        )
        rows = rows[u_h : u_h + page_limit]

    page = materialize_search_page(
        page_rows=rows,
        pool=pool_h,
        u=u_h,
        page_limit=page_limit,
        return_type=return_type,
        return_fields=return_fields,
        model_type=model_type,
    )

    if return_count:
        return page_from_limit_offset(
            page,
            pagination_dict,
            total=total,
            snapshot=handle_h,
        )

    return page_from_limit_offset(
        page,
        pagination_dict,
        total=None,
        snapshot=handle_h,
    )
