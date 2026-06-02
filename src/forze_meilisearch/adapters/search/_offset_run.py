"""Offset pagination execution for Meilisearch search."""

from __future__ import annotations

from typing import Any, Sequence

from pydantic import BaseModel

from forze.application.contracts.base import page_from_limit_offset
from forze.application.contracts.querying import (
    PaginationExpression,
    QueryFilterExpression,
)
from forze.application.contracts.search import (
    SearchOptions,
    SearchResultSnapshotOptions,
    SearchSpec,
    effective_phrase_combine,
    normalize_search_queries,
)
from forze.application.integrations.search import SearchResultSnapshot
from forze.base.primitives import JsonDict
from forze.base.serialization import default_model_codec
from forze_meilisearch.adapters.search._search_params import (
    attributes_to_search_on,
    build_search_query_string,
    build_sort,
    render_user_sorts,
)
from forze_meilisearch.adapters.search.base import MeilisearchSearchGateway
from forze_meilisearch.kernel.client.port import MeilisearchClientPort

# ----------------------- #


def _offset_from_dict(pagination_dict: dict[str, Any]) -> int:
    raw = pagination_dict.get("offset")
    return 0 if raw is None else int(raw)


def _limit_from_dict(pagination_dict: dict[str, Any]) -> int | None:
    raw = pagination_dict.get("limit")
    return None if raw is None else int(raw)


async def execute_meilisearch_offset_search[M: BaseModel](
    gw: MeilisearchSearchGateway[M],
    *,
    client: MeilisearchClientPort,
    query: str | Sequence[str],
    filters: QueryFilterExpression | None,
    spec: SearchSpec[Any],
    variant: str,
    fingerprint_extras: dict[str, object] | None,
    pagination: PaginationExpression | None,
    snapshot: SearchResultSnapshotOptions | None,
    options: SearchOptions | None,
    sorts: Any,
    return_count: bool,
    return_type: type[BaseModel] | None,
    return_fields: Sequence[str] | None,
    result_snapshot: SearchResultSnapshot | None,
) -> Any:
    terms = tuple(normalize_search_queries(query))
    combine = effective_phrase_combine(options)
    q = build_search_query_string(terms, combine=combine)

    fp_fingerprint = SearchResultSnapshot.simple_search_fingerprint(
        query,
        filters,
        sorts,
        spec_name=spec.name,
        variant=variant,
        extras=fingerprint_extras,
    )

    pagination_dict: dict[str, Any] = dict(pagination or {})
    rs_spec = spec.snapshot

    if result_snapshot is not None and rs_spec is not None:
        maybe_snap: Any = await result_snapshot.read_simple_result_snapshot(
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

    filter_str = gw.build_filter(filters)
    attrs = attributes_to_search_on(spec, options, gw.field_map)
    sort_list = build_sort(render_user_sorts(sorts, gw.field_map))

    offset = _offset_from_dict(pagination_dict)
    limit = _limit_from_dict(pagination_dict)

    search_kwargs: dict[str, Any] = {}

    if filter_str is not None:
        search_kwargs["filter"] = filter_str

    if attrs is not None:
        search_kwargs["attributes_to_search_on"] = attrs

    if sort_list is not None:
        search_kwargs["sort"] = sort_list

    if offset:
        search_kwargs["offset"] = offset

    if limit is not None:
        search_kwargs["limit"] = limit

    if return_fields is not None:
        phys_fields = gw.physical_paths(return_fields)
        search_kwargs["attributes_to_retrieve"] = list(
            dict.fromkeys([*phys_fields, gw.primary_key])
        )

    index = client.index(
        await gw._resolved_index_uid()  # pyright: ignore[reportPrivateUsage]
    )
    result = await index.search(q, **search_kwargs)

    hits_raw = list(getattr(result, "hits", []) or [])
    total = int(
        getattr(result, "estimated_total_hits", None)
        or getattr(result, "total_hits", None)
        or len(hits_raw)
    )

    rows = [gw.from_hit(dict(h)) for h in hits_raw]

    if return_fields is not None:
        page_rows: list[JsonDict] = [
            {k: r.get(k, None) for k in return_fields} for r in rows
        ]
    elif return_type is not None:
        page_rows = default_model_codec(return_type).decode_mapping_many(rows)  # type: ignore[assignment]
    else:
        page_rows = gw.spec.resolved_read_codec.decode_mapping_many(rows)  # type: ignore[assignment]

    want_snap = (
        result_snapshot is not None
        and rs_spec is not None
        and result_snapshot.should_write_result_snapshot(snapshot, rs_spec)
    )

    handle_out = None

    if want_snap and result_snapshot is not None and rs_spec is not None:
        pool_models = gw.spec.resolved_read_codec.decode_mapping_many(rows)
        handle_out = await result_snapshot.put_simple_ordered_hits(
            pool_models,
            snap_opt=snapshot,
            rs_spec=rs_spec,
            fp_computed=fp_fingerprint,
            pool_len_before_cap=total,
        )

    if return_count:
        return page_from_limit_offset(
            page_rows,
            pagination_dict,
            total=total,
            snapshot=handle_out,
        )

    return page_from_limit_offset(
        page_rows,
        pagination_dict,
        total=None,
        snapshot=handle_out,
    )
