"""PGroonga plan selection, candidate caps, and read/heap coalescing helpers."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping, Sequence
from typing import TYPE_CHECKING, Any, Literal

from forze.application.contracts.querying import QueryExpr
from forze.application.contracts.resolution import is_static_relation
from forze.application.contracts.search import PgroongaPlan, SearchOptions
from forze.domain.constants import ID_FIELD
from forze_postgres.kernel.relation import RelationSpec

if TYPE_CHECKING:
    from forze.application.contracts.search import SearchResultSnapshotOptions
    from forze.application.integrations.search import SearchResultSnapshot
    from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
    from forze_postgres.kernel.gateways import PostgresQualifiedName

# ----------------------- #

ResolvedPgroongaPlan = Literal["filter_first", "index_first"]

_DEFAULT_CANDIDATE_MARGIN = 50

# ....................... #


def is_trivial_filter(parsed: QueryExpr | None) -> bool:
    """Return whether the parsed filter AST is absent (filterless browse semantics)."""

    return parsed is None


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


def effective_pgroonga_plan_option(
    options: SearchOptions | None,
) -> PgroongaPlan | None:
    raw = (options or {}).get("pgroonga_plan")

    if raw in ("filter_first", "index_first", "auto"):
        return raw

    return None


def effective_candidate_limit(
    *,
    config_limit: int | None,
    options: SearchOptions | None,
    pagination: Mapping[str, Any] | None,
    snapshot: SearchResultSnapshotOptions | None,
    result_snapshot: SearchResultSnapshot | None,
    rs_spec: Any | None,
) -> int | None:
    """Resolve the ranked-row cap for PGroonga pipelines (``None`` disables)."""

    opt_raw = (options or {}).get("candidate_limit")

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
    options: SearchOptions | None,
    parsed_filters: QueryExpr | None,
    read_qname: PostgresQualifiedName,
    introspector: PostgresIntrospector,
    auto_index_first_min_rows: int,
    auto_use_exact_count: bool,
    count_filtered_rows: Callable[[], Awaitable[int]] | None = None,
) -> ResolvedPgroongaPlan:
    """Pick ``filter_first`` or ``index_first`` for one ranked PGroonga query."""

    plan = effective_pgroonga_plan_option(options) or configured

    if plan == "filter_first":
        return "filter_first"

    if plan == "index_first":
        return "index_first"

    if not is_trivial_filter(parsed_filters):
        return "filter_first"

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
