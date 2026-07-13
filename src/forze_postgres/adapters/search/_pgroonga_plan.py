"""PGroonga plan selection, candidate caps, and read/heap coalescing helpers."""

from __future__ import annotations

import math
from collections.abc import Awaitable, Callable, Mapping, Sequence
from typing import TYPE_CHECKING, Any, Literal, cast

from forze.application.contracts.querying import QueryAnd, QueryExpr, QueryField
from forze.application.contracts.resolution import is_static_relation
from forze.application.contracts.search import SearchOptions
from forze.domain.constants import ID_FIELD
from forze_postgres.kernel.relation import RelationSpec

if TYPE_CHECKING:
    from forze.application.contracts.search import (
        MultiSourceSearchOptions,
        SearchResultSnapshotOptions,
    )
    from forze.application.integrations.search import SearchResultSnapshot
    from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
    from forze_postgres.kernel.gateways import PostgresQualifiedName

# ----------------------- #

PgroongaPlan = Literal["filter_first", "index_first", "auto"]
"""PGroonga ranked search SQL shape (Postgres adapter config; resolved per query)."""

ResolvedPgroongaPlan = Literal["filter_first", "index_first"]

_DEFAULT_CANDIDATE_MARGIN = 50

_INDEX_FIRST_ELIGIBLE_OPS = frozenset({"$eq", "$neq", "$in", "$nin"})

# ....................... #


def is_trivial_filter(parsed: QueryExpr | None) -> bool:
    """Return whether the parsed filter AST is absent (filterless browse semantics)."""

    return parsed is None


def is_index_first_eligible_filter(parsed: QueryExpr | None) -> bool:
    """Return whether filters are safe as post-filters after a heap top-K (conservative)."""

    if parsed is None:
        return True

    if isinstance(parsed, QueryAnd):
        return all(is_index_first_eligible_filter(item) for item in parsed.items)

    if isinstance(parsed, QueryField):
        return parsed.op in _INDEX_FIRST_ELIGIBLE_OPS

    return False


def is_coalesced_read_heap(
    read: RelationSpec,
    heap: RelationSpec,
    join_pairs: Sequence[tuple[str, str]] | None,
) -> bool:
    """True when read and heap are the same static relation with default id join."""

    if heap is not None and not is_static_relation(heap):
        return False

    if not is_static_relation(read):
        return False

    heap_spec = (
        heap
        if heap is not None  # pyright: ignore[reportUnnecessaryComparison]
        else read
    )

    if tuple(read) != tuple(heap_spec):
        return False

    pairs = join_pairs if join_pairs is not None else ((ID_FIELD, ID_FIELD),)

    return pairs == ((ID_FIELD, ID_FIELD),)


def effective_ranked_candidate_limit(
    *,
    config_limit: int | None,
    options: SearchOptions | None,
    pagination: Mapping[str, Any] | None,
    snapshot: SearchResultSnapshotOptions | None,
    result_snapshot: SearchResultSnapshot | None,
    rs_spec: Any | None,
) -> int | None:
    """Resolve the ranked-row cap for PGroonga, FTS, and vector pipelines (``None`` disables)."""

    opt_raw = (options or {}).get("max_candidates")

    if opt_raw is not None:
        cap = int(opt_raw)

    elif config_limit is not None:
        cap = int(config_limit)

    else:
        return None

    pag = dict(pagination or {})
    page_need = int(pag.get("limit") or 0) + int(pag.get("offset") or 0)

    if page_need > 0:
        cap = max(cap, page_need + _DEFAULT_CANDIDATE_MARGIN)

    if (
        result_snapshot is not None
        and rs_spec is not None
        and result_snapshot.should_write_result_snapshot(snapshot, rs_spec)
    ):
        max_ids = result_snapshot.effective_snapshot_max_ids(snapshot, rs_spec)
        cap = max(cap, int(max_ids))

    return max(cap, 1)


def effective_candidate_limit(
    *,
    config_limit: int | None,
    options: SearchOptions | None,
    pagination: Mapping[str, Any] | None,
    snapshot: SearchResultSnapshotOptions | None,
    result_snapshot: SearchResultSnapshot | None,
    rs_spec: Any | None,
) -> int | None:
    """Alias for :func:`effective_ranked_candidate_limit` (PGroonga naming)."""

    return effective_ranked_candidate_limit(
        config_limit=config_limit,
        options=options,
        pagination=pagination,
        snapshot=snapshot,
        result_snapshot=result_snapshot,
        rs_spec=rs_spec,
    )


def effective_combo_limit(
    *,
    config_limit: int | None,
    per_leg_limit: int,
    options: SearchOptions | None,
    pagination: Mapping[str, Any] | None,
    snapshot: SearchResultSnapshotOptions | None,
    result_snapshot: SearchResultSnapshot | None,
    rs_spec: Any | None,
) -> int | None:
    """Resolve hub ``combo_top`` cap (``None`` disables the extra CTE)."""

    # ``merge_candidates`` lives on ``MultiSourceSearchOptions`` (hub/federated); the param is
    # typed as the base ``SearchOptions``, so view it as the multi-source shape to read the key.
    opt_raw = cast("MultiSourceSearchOptions", options or {}).get("merge_candidates")

    if opt_raw is not None:
        cap = int(opt_raw)

    elif config_limit is not None:
        cap = int(config_limit)

    else:
        cap = max(int(per_leg_limit), 1)
        pag = dict(pagination or {})
        page_need = int(pag.get("limit") or 0) + int(pag.get("offset") or 0)

        if page_need > 0:
            cap = max(cap, page_need + _DEFAULT_CANDIDATE_MARGIN)

        if (
            result_snapshot is not None
            and rs_spec is not None
            and result_snapshot.should_write_result_snapshot(snapshot, rs_spec)
        ):
            max_ids = result_snapshot.effective_snapshot_max_ids(snapshot, rs_spec)
            cap = max(cap, int(max_ids))

        return max(cap, 1)

    pag = dict(pagination or {})
    page_need = int(pag.get("limit") or 0) + int(pag.get("offset") or 0)

    if page_need > 0:
        cap = max(cap, page_need + _DEFAULT_CANDIDATE_MARGIN)

    if (
        result_snapshot is not None
        and rs_spec is not None
        and result_snapshot.should_write_result_snapshot(snapshot, rs_spec)
    ):
        max_ids = result_snapshot.effective_snapshot_max_ids(snapshot, rs_spec)
        cap = max(cap, int(max_ids))

    return max(cap, 1)


def index_first_heap_limit(
    candidate_cap: int,
    *,
    has_projection_filters: bool,
    filter_margin: float,
) -> int:
    """Heap ``LIMIT`` for index-first (overshoot when projection post-filters shrink rows)."""

    cap = max(int(candidate_cap), 1)

    if not has_projection_filters:
        return cap

    margin = max(float(filter_margin), 1.0)

    return max(cap, math.ceil(cap * margin))


def ensure_pgroonga_plan_with_candidate_cap(
    resolved_plan: ResolvedPgroongaPlan,
    candidate_cap: int | None,
) -> ResolvedPgroongaPlan:
    """``index_first`` always applies a ranked-row ``LIMIT``; without a cap use ``filter_first``."""

    if resolved_plan == "index_first" and candidate_cap is None:
        return "filter_first"

    return resolved_plan


async def resolve_pgroonga_plan(
    *,
    configured: PgroongaPlan,
    parsed_filters: QueryExpr | None,
    read_qname: PostgresQualifiedName,
    introspector: PostgresIntrospector,
    auto_index_first_min_rows: int,
    auto_filter_first_max_rows: int,
    auto_with_filters: bool,
    auto_use_exact_count: bool,
    count_filtered_rows: Callable[[], Awaitable[int]] | None = None,
    estimate_filtered_rows: Callable[[], Awaitable[int]] | None = None,
    tenant_aware: bool = False,
) -> ResolvedPgroongaPlan:
    """Pick ``filter_first`` or ``index_first`` for one ranked PGroonga query."""

    # Tenant isolation must always narrow *before* ranking. ``index_first`` takes a
    # heap top-K across all tenants and applies the tenant predicate only in the
    # outer post-filter, which both scans cross-tenant rows and silently truncates a
    # tenant's results (its rows are a small slice of a global top-K). The tenant
    # predicate is injected outside ``parsed_filters`` (see ``where_clause`` ->
    # ``_add_tenant_where``), so it never makes a filterless browse look non-trivial
    # to the heuristics below. Force ``filter_first`` so the tenant predicate, carried
    # in the filtered-CTE ``WHERE``, always narrows first — overriding even an explicit
    # ``index_first``. Skipping the estimation queries here also avoids the misleading
    # whole-table row estimate on a multi-tenant relation.
    if tenant_aware:
        return "filter_first"

    plan = configured

    if plan == "filter_first":
        return "filter_first"

    if plan == "index_first":
        return "index_first"

    if is_trivial_filter(parsed_filters):
        if auto_use_exact_count and count_filtered_rows is not None:
            filtered_count = await count_filtered_rows()

            if filtered_count >= auto_index_first_min_rows:
                return "index_first"

            return "filter_first"

        estimate = await introspector.estimate_relation_rows(
            schema=read_qname.schema,
            relation=read_qname.name,
        )

        if estimate >= auto_index_first_min_rows:
            return "index_first"

        return "filter_first"

    if not auto_with_filters or not is_index_first_eligible_filter(parsed_filters):
        return "filter_first"

    filtered_estimate: int | None = None

    if auto_use_exact_count and count_filtered_rows is not None:
        filtered_estimate = await count_filtered_rows()

    elif estimate_filtered_rows is not None:
        filtered_estimate = await estimate_filtered_rows()

    if filtered_estimate is not None:
        if filtered_estimate <= auto_filter_first_max_rows:
            return "filter_first"

        if filtered_estimate >= auto_index_first_min_rows:
            return "index_first"

        return "filter_first"

    if estimate_filtered_rows is not None:
        filtered_estimate = await estimate_filtered_rows()

        if filtered_estimate <= auto_filter_first_max_rows:
            return "filter_first"

        if filtered_estimate >= auto_index_first_min_rows:
            return "index_first"

    return "filter_first"
