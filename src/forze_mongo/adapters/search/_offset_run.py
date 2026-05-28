"""Offset pagination execution for Mongo ranked search."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from pydantic import BaseModel

from forze.application.contracts.base import page_from_limit_offset
from forze.application.contracts.querying import (
    PaginationExpression,
    QueryFilterExpression,
)
from forze.application.contracts.search import (
    SearchResultSnapshotOptions,
    SearchSpec,
)
from forze.application.coordinators import SearchResultSnapshotCoordinator
from forze.base.primitives import JsonDict
from forze.base.serialization import pydantic_validate_many
from forze_mongo.kernel.platform.port import MongoClientPort

from ._materialize import materialize_search_page
from ._pipeline import append_pagination_stages, build_count_pipeline
from .base import MongoSearchGateway

# ----------------------- #


def _offset_from_dict(pagination_dict: dict[str, Any]) -> int:
    raw = pagination_dict.get("offset")

    if raw is None:
        return 0

    return int(raw)


# ....................... #


async def execute_mongo_ranked_offset_search[M: BaseModel](
    gw: MongoSearchGateway[M],
    *,
    client: MongoClientPort,
    ranked_pipeline: list[JsonDict],
    query: str | Sequence[str],
    filters: QueryFilterExpression | None,  # type: ignore[valid-type]
    spec: SearchSpec[Any],
    variant: str,
    fingerprint_extras: dict[str, object] | None,
    pagination: PaginationExpression | None,
    snapshot: SearchResultSnapshotOptions | None,
    return_count: bool,
    return_type: type[BaseModel] | None,
    return_fields: Sequence[str] | None,
    snapshot_coord: SearchResultSnapshotCoordinator | None,
) -> Any:
    """Run count (optional), aggregation fetch, and snapshot materialization."""

    rs_spec = spec.snapshot
    fp_fingerprint = SearchResultSnapshotCoordinator.simple_search_fingerprint(
        query,
        filters,
        None,
        spec_name=spec.name,
        variant=variant,
        extras=fingerprint_extras,
    )

    pagination_dict: dict[str, Any] = dict(pagination or {})

    if snapshot_coord is not None and rs_spec is not None:
        maybe_snap: Any = await snapshot_coord.read_simple_result_snapshot(
            rs_spec=rs_spec,
            snap_opt=snapshot,
            fp_computed=fp_fingerprint,
            spec=spec,
            pagination=pagination_dict,
            return_type=return_type,
            return_fields=return_fields,
            return_count=return_count,
        )

        if maybe_snap is not None:
            return maybe_snap

    coll = await gw.coll()
    total = 0

    if return_count:
        count_rows = await client.aggregate(
            coll,
            build_count_pipeline(ranked_pipeline),
            limit=1,
        )

        total = int(count_rows[0]["total"]) if count_rows else 0
        any_hits: list[Any] = []

        if total == 0:
            return page_from_limit_offset(any_hits, pagination_dict, total=0)

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
            want_snap,
            max_nw,
            pagination_dict,
        )
    )

    offset = sql_offset if want_snap else _offset_from_dict(pagination_dict)
    limit = sql_limit if want_snap else pagination_dict.get("limit")

    data_pipeline = append_pagination_stages(
        ranked_pipeline,
        offset=offset,
        limit=int(limit) if limit is not None else None,
    )

    rows = await client.aggregate(coll, data_pipeline, limit=None)
    normalized = [
        gw._from_storage_doc(r) for r in rows  # pyright: ignore[reportPrivateUsage]
    ]

    handle_out = None
    pool_snap: list[M] | None = None
    u_off = offset if want_snap else _offset_from_dict(pagination_dict)

    if want_snap and snapshot_coord is not None and rs_spec is not None:
        pool_len = len(normalized)
        pool_snap = pydantic_validate_many(gw.model_type, normalized)
        handle_out = await snapshot_coord.put_simple_ordered_hits(
            pool_snap,
            snap_opt=snapshot,
            rs_spec=rs_spec,
            fp_computed=fp_fingerprint,
            pool_len_before_cap=pool_len,
        )

        if want_snap and sql_limit is not None:
            normalized = normalized[u_off : u_off + page_limit]

    page = materialize_search_page(
        page_rows=normalized,
        pool=pool_snap,
        u=u_off,
        page_limit=(
            page_limit
            if want_snap
            else (int(limit) if limit is not None else len(normalized))
        ),
        return_type=return_type,
        return_fields=return_fields,
        model_type=gw.model_type,
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
