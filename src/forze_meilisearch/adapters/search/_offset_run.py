"""Offset pagination execution for Meilisearch search."""

from __future__ import annotations

from typing import Any, Sequence

import attrs
from pydantic import BaseModel

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
from forze.application.integrations.search.offset_executor import (
    OffsetFetchWindow,
    OffsetRowsResult,
    execute_simple_offset_search_with_snapshot,
    offset_from_dict,
)
from forze.base.exceptions import exc
from forze_meilisearch.adapters.search._facets_highlights import (
    FacetPlan,
    HighlightPlan,
    extract_facets,
    extract_highlights,
    plan_facets,
    plan_highlights,
)
from forze_meilisearch.adapters.search._search_params import (
    attributes_to_search_on,
    build_search_query_string,
    build_sort,
    render_user_sorts,
)
from forze_meilisearch.adapters.search.base import MeilisearchSearchGateway
from forze_meilisearch.kernel.client.port import MeilisearchClientPort

# ----------------------- #


@attrs.define(slots=True)
class _MeilisearchOffsetHooks:
    gw: MeilisearchSearchGateway[Any]
    client: MeilisearchClientPort
    query_string: str
    filter_str: str | None
    attrs: list[str] | None
    sort_list: list[str] | None
    pagination_dict: dict[str, Any]
    return_count: bool
    return_fields: Sequence[str] | None
    facet_plan: FacetPlan | None = None
    highlight_plan: HighlightPlan | None = None

    async def fetch_count(self) -> int | None:
        return None

    async def fetch_rows(
        self,
        window: OffsetFetchWindow,
        *,
        want_snap: bool,
    ) -> OffsetRowsResult:
        search_kwargs: dict[str, Any] = {}

        if self.filter_str is not None:
            search_kwargs["filter"] = self.filter_str

        if self.attrs is not None:
            search_kwargs["attributes_to_search_on"] = self.attrs

        if self.sort_list is not None:
            search_kwargs["sort"] = self.sort_list

        if self.facet_plan is not None:
            search_kwargs["facets"] = self.facet_plan.physical_fields

        if self.highlight_plan is not None:
            search_kwargs["attributes_to_highlight"] = self.highlight_plan.physical_fields
            search_kwargs["highlight_pre_tag"] = self.highlight_plan.pre_tag
            search_kwargs["highlight_post_tag"] = self.highlight_plan.post_tag

        if want_snap:
            offset = window.fetch_offset
            limit = window.fetch_limit

            if offset:
                search_kwargs["offset"] = offset

            if limit is not None:
                search_kwargs["limit"] = limit

        else:
            offset = offset_from_dict(self.pagination_dict)
            raw_limit = self.pagination_dict.get("limit")
            limit = int(raw_limit) if raw_limit is not None else None

            if offset:
                search_kwargs["offset"] = offset

            if limit is not None:
                search_kwargs["limit"] = limit

        # Meilisearch caps a query at ``maxTotalHits`` (index setting, default 1000):
        # a window reaching past it comes back silently short. Fail closed so deep
        # pagination / snapshot builds don't quietly drop rows.
        max_total_hits = self.gw.config.max_total_hits
        far_edge = offset + (limit if limit is not None else 0)

        if far_edge > max_total_hits:
            raise exc.precondition(
                f"Requested window (offset {offset} + limit {limit}) exceeds "
                f"Meilisearch maxTotalHits ({max_total_hits}); Meilisearch would "
                "silently truncate. Narrow the query or raise the index's "
                "maxTotalHits and this route's max_total_hits.",
                code="core.search.max_total_hits_exceeded",
            )

        if self.return_fields is not None:
            phys_fields = self.gw.physical_paths(self.return_fields)
            search_kwargs["attributes_to_retrieve"] = list(
                dict.fromkeys([*phys_fields, self.gw.primary_key])
            )

        index = self.client.index(
            await self.gw._resolved_index_uid()  # pyright: ignore[reportPrivateUsage]
        )
        result = await index.search(self.query_string, **search_kwargs)

        hits_raw = [dict(h) for h in getattr(result, "hits", []) or []]
        total = int(
            getattr(result, "estimated_total_hits", None)
            or getattr(result, "total_hits", None)
            or len(hits_raw)
        )
        rows = [self.gw.from_hit(h) for h in hits_raw]

        facets = (
            extract_facets(result, self.facet_plan)
            if self.facet_plan is not None
            else None
        )
        highlights = (
            extract_highlights(hits_raw, self.highlight_plan)
            if self.highlight_plan is not None
            else None
        )

        return OffsetRowsResult(
            rows=rows,
            total=total if self.return_count else None,
            facets=facets,
            highlights=highlights,
        )


# ....................... #


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

    filter_str = gw.build_filter(filters)
    search_attrs = attributes_to_search_on(spec, options, gw.field_map)
    sort_list = build_sort(render_user_sorts(sorts, gw.field_map))
    pagination_dict: dict[str, Any] = dict(pagination or {})
    facet_plan = plan_facets(gw, spec, options)
    highlight_plan = plan_highlights(gw, spec, options)

    # Facets/highlights ride the live page, not the id-only snapshot; a replay would silently
    # drop them. Disable snapshot reuse for those requests so each page runs live.
    if facet_plan is not None or highlight_plan is not None:
        result_snapshot = None

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
        return_type=return_type,
        return_fields=return_fields,
        model_type=gw.spec.model_type,
        codec=gw.spec.resolved_read_codec,
        result_snapshot=result_snapshot,
        hooks=_MeilisearchOffsetHooks(
            gw=gw,
            client=client,
            query_string=q,
            filter_str=filter_str,
            attrs=search_attrs,
            sort_list=sort_list,
            pagination_dict=pagination_dict,
            return_count=return_count,
            return_fields=return_fields,
            facet_plan=facet_plan,
            highlight_plan=highlight_plan,
        ),
    )
