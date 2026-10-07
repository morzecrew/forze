"""Offset pagination execution for Meilisearch search."""

from __future__ import annotations

import re
from collections.abc import Collection, Sequence
from typing import Any

import attrs
from meilisearch_python_sdk.errors import MeilisearchApiError
from pydantic import BaseModel

from forze.application.contracts.querying import (
    PaginationExpression,
    QueryFilterExpression,
    QuerySortExpression,
)
from forze.application.contracts.search import (
    SearchOptions,
    SearchResultSnapshotOptions,
    SearchSpec,
    effective_phrase_combine,
    normalize_search_queries,
    resolve_search_sorts,
)
from forze.application.integrations.search import SearchResultSnapshot
from forze.application.integrations.search.offset_executor import (
    OffsetFetchWindow,
    OffsetRowsResult,
    execute_simple_offset_search_with_snapshot,
    offset_from_dict,
)
from forze.base.exceptions import CoreException, exc
from forze.domain.constants import ID_FIELD
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
    sortable_attributes,
)
from forze_meilisearch.adapters.search.base import (
    _DECIMAL_EXACT_FIELD,  # pyright: ignore[reportPrivateUsage]
    MeilisearchSearchGateway,
)
from forze_meilisearch.kernel.client.port import MeilisearchClientPort

# ----------------------- #

# Meilisearch applies this default page size when a search omits ``limit`` — so a limitless query
# still reads ``offset + 20`` rows, which the ``maxTotalHits`` guard must count (else it undercounts
# the window and lets a query slip past the cap into silent truncation).
_MEILI_DEFAULT_SEARCH_LIMIT = 20

# ----------------------- #

_UNSORTABLE = re.compile(r"Attribute `([^`]+)` is not sortable")
"""The attribute in Meilisearch's refusal; the same message also lists the sortable ones."""


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
    spec_sort: tuple[str, ...] = ()
    """Sort attributes the spec added rather than the request: its default and the id."""

    async def fetch_count(self) -> int | None:
        # By default the total comes cheaply from the search result's ``estimatedTotalHits``
        # (see ``fetch_rows``). When the route opts into exact counts, run one extra page-mode
        # query — Meilisearch's ``totalHits`` is exact (bounded by ``maxTotalHits``).
        if not (self.return_count and self.gw.config.exact_total_count):
            return None

        kwargs: dict[str, Any] = {"hits_per_page": 1, "page": 1}

        if self.filter_str is not None:
            kwargs["filter"] = self.filter_str

        if self.attrs is not None:
            # The count must count what the rows query matches: without the same
            # ``attributes_to_search_on`` narrowing, the exact total counts matches
            # across EVERY searchable attribute while the page searches a subset.
            kwargs["attributes_to_search_on"] = self.attrs

        index = self.client.index(
            await self.gw._resolved_index_uid()  # pyright: ignore[reportPrivateUsage]
        )
        result = await index.search(self.query_string, **kwargs)
        total = getattr(result, "total_hits", None)

        return int(total) if total is not None else None

    def _unsortable(self, error: MeilisearchApiError) -> CoreException | None:
        return unsortable_refusal(error, self.spec_sort)

    # ....................... #

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
        # A missing ``limit`` still reads Meilisearch's default page, so count that toward the
        # window — otherwise the guard undercounts and a deep offset slips past ``maxTotalHits``.
        effective_limit = limit if limit is not None else _MEILI_DEFAULT_SEARCH_LIMIT
        far_edge = offset + effective_limit

        if far_edge > max_total_hits:
            raise exc.precondition(
                f"Requested window (offset {offset} + limit {effective_limit}) exceeds "
                f"Meilisearch maxTotalHits ({max_total_hits}); Meilisearch would "
                "silently truncate. Narrow the query or raise the index's "
                "maxTotalHits and this route's max_total_hits.",
                code="core.search.max_total_hits_exceeded",
            )

        # A snapshot window feeds the pool, which keys every hit by its whole record, so it
        # retrieves every attribute; only the page returned is projected.
        if self.return_fields is not None and not want_snap:
            phys_fields = self.gw.physical_paths(self.return_fields)
            # The exact-decimal shadow rides every projection: without it a projected
            # read silently returns the f64-rounded index number for a Decimal field
            # while an unprojected read of the same row returns the exact value.
            # Harmless when the model has no Decimals (the attribute simply doesn't
            # exist on the document).
            search_kwargs["attributes_to_retrieve"] = list(
                dict.fromkeys([*phys_fields, self.gw.primary_key, _DECIMAL_EXACT_FIELD])
            )

        index = self.client.index(
            await self.gw._resolved_index_uid()  # pyright: ignore[reportPrivateUsage]
        )
        try:
            result = await index.search(self.query_string, **search_kwargs)

        except MeilisearchApiError as e:
            if (refusal := self._unsortable(e)) is not None:
                raise refusal from e

            raise

        hits_raw = [dict(h) for h in getattr(result, "hits", []) or []]
        total = int(
            getattr(result, "estimated_total_hits", None)
            or getattr(result, "total_hits", None)
            or len(hits_raw)
        )
        rows = [self.gw.from_hit(h) for h in hits_raw]

        facets = extract_facets(result, self.facet_plan) if self.facet_plan is not None else None
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


def unsortable_refusal(
    error: MeilisearchApiError,
    spec_sort: Collection[str],
) -> CoreException | None:
    """Who is at fault when an index cannot sort by an attribute a page asked for.

    One the spec added (*spec_sort*: its default or the id) is a configuration error, raised
    loud rather than retried without the sort: an index provisioned before the spec set a
    ``default_sort``, or managed outside forze, would otherwise answer in an order the spec
    does not promise. One the request named is the caller's. ``None`` for any other error.
    """

    if error.code != "invalid_search_sort":
        return None

    named = _UNSORTABLE.search(error.message)

    if named is None or named.group(1) not in spec_sort:
        return exc.precondition(f"Meilisearch refused the sort: {error.message}")

    return exc.configuration(
        f"The Meilisearch index cannot sort by {named.group(1)!r}, which the search spec "
        "orders an unsorted page by: re-run ensure_index, or add it to sortable_attributes. "
        f"{error.message}",
    )


# ....................... #


def page_order(
    gw: MeilisearchSearchGateway[Any],
    spec: SearchSpec[Any],
    sorts: QuerySortExpression | None,
    *,
    ranked: bool,
) -> QuerySortExpression:
    """The order a page sorts by, after the engine's relevance.

    With search text, only the request's own sorts: Meilisearch applies ``sort`` before its
    ``exactness`` rule, so a default or an id there would settle every relevance tie first,
    and relevance ties keep the engine's order. A blank query takes the request's sorts or
    else ``default_sort``, then the id when the index can sort by it.
    """

    sortable_id = gw.primary_key in sortable_attributes(spec, gw.config)

    return resolve_search_sorts(
        sorts,
        default_sort=None if ranked else spec.default_sort,
        read_fields=spec.stored_read_fields,
        # The id closes a blank page's order, where the index can sort by it.
        tiebreaker=ID_FIELD if sortable_id and not ranked else None,
        model=spec.model_type,
        spec_name=spec.name,
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
    order = page_order(gw, spec, sorts, ranked=bool(terms))
    sort_list = build_sort(render_user_sorts(order, gw.config))
    requested = {attr for attr, _ in render_user_sorts(sorts, gw.config)}
    sorted_by = [entry.rsplit(":", 1)[0] for entry in sort_list or ()]
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
        # The snapshot is keyed on the order the engine is asked for, which a ranked page
        # takes from the request alone.
        fingerprint_sorts=order,
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
            spec_sort=tuple(attr for attr in sorted_by if attr not in requested),
        ),
    )
