"""Canonical per-request hub search plan (semantics + execution hint)."""

from __future__ import annotations

from typing import Any, Literal, Sequence

import attrs

from forze.application.contracts.querying import QuerySortExpression
from forze.application.contracts.search import (
    HubSearchSpec,
    SearchOptions,
    SearchResultSnapshotOptions,
    normalize_search_queries,
    prepare_hub_search_options,
)
from forze.application.integrations.search import SearchResultSnapshot

from .._pgroonga_plan import effective_combo_limit
from .._search_count import effective_search_count
from ._typing_host import HubSearchHost
from .constants import HUB_RANK
from .runtime import HubLegRuntime
from .semantics import HubCombine, HubScoreMerge, hub_order_key_spec

# ----------------------- #

HubSearchMode = Literal["offset", "cursor"]

# ....................... #


@attrs.define(frozen=True, slots=True, kw_only=True)
class HubSearchPlan:
    """Resolved hub search semantics for one request."""

    terms: tuple[str, ...]
    do_legs: bool
    active: tuple[tuple[int, HubLegRuntime, float], ...]
    leg_options: SearchOptions
    member_weights_list: tuple[float, ...]
    combine: HubCombine
    score_merge: HubScoreMerge
    read_fields: frozenset[str]
    rank_field: str
    per_leg_limit: int
    resolved_combo: int | None
    effective_sorts: QuerySortExpression | None  # type: ignore[valid-type]
    order_key_spec: tuple[tuple[str, str], ...]
    use_parallel: bool
    count_policy: str
    execution: Literal["sql", "parallel"]


# ....................... #


def hub_members_weighted(
    hub_spec: HubSearchSpec[Any],
    member_weights_list: Sequence[float],
) -> list[tuple[str, float]]:
    return [
        (hub_spec.members[i].name, float(member_weights_list[i]))
        for i in range(len(hub_spec.members))
    ]


# ....................... #


async def build_hub_search_plan(
    host: HubSearchHost[Any],
    *,
    query: str | Sequence[str],
    options: SearchOptions | None,
    sorts: QuerySortExpression | None,  # type: ignore[valid-type]
    pagination_or_cursor: dict[str, Any],
    snapshot: SearchResultSnapshotOptions | None,
    result_snapshot: SearchResultSnapshot | None,
    mode: HubSearchMode,
) -> HubSearchPlan:
    """Build canonical hub search plan for SQL or parallel execution."""

    terms = tuple(normalize_search_queries(query))
    leg_options, member_weights_list = prepare_hub_search_options(
        host.hub_spec,
        options,
    )
    weights_tuple = tuple(float(w) for w in member_weights_list)

    active = tuple(
        (i, leg, float(member_weights_list[i]))
        for i, leg in enumerate(host.members)
        if member_weights_list[i] > 0.0
    )
    do_legs = bool(terms) and bool(active)

    execution = getattr(host, "execution", "sql")
    if execution not in ("sql", "parallel"):
        execution = "sql"

    use_parallel = (
        execution == "parallel"
        and do_legs
        and (mode == "cursor" or sorts is None)
    )

    hub_spec = host.hub_spec
    effective_sorts = sorts if sorts else hub_spec.default_sort
    rs_spec = hub_spec.snapshot

    # Cursor pagination walks the whole ranked set one keyset page at a time, so the
    # ``combo_top`` cap — a top-N candidate-pool bound for a single offset page — must not
    # apply: capping it truncates a deep cursor walk (and a hub stream export) at
    # ``combo_top`` instead of the full result set. The cap stays on the offset path.
    resolved_combo = (
        None
        if mode == "cursor"
        else effective_combo_limit(
            config_limit=getattr(host, "combo_limit", None),
            per_leg_limit=host.per_leg_limit,
            options=leg_options,
            pagination=pagination_or_cursor,
            snapshot=snapshot,
            result_snapshot=result_snapshot,
            rs_spec=rs_spec,
        )
    )

    key_spec = hub_order_key_spec(
        do_legs=do_legs,
        sorts=sorts,
        default_sort=hub_spec.default_sort,
        read_fields=host.read_fields,
        spec_name=hub_spec.name,
        rank_field=HUB_RANK,
        model=host.model_type,
    )

    return HubSearchPlan(
        terms=terms,
        do_legs=do_legs,
        active=active,
        leg_options=leg_options,
        member_weights_list=weights_tuple,
        combine=host.combine,  # type: ignore[arg-type]
        score_merge=host.score_merge,  # type: ignore[arg-type]
        read_fields=host.read_fields,
        rank_field=HUB_RANK,
        per_leg_limit=host.per_leg_limit,
        resolved_combo=resolved_combo if do_legs else None,
        effective_sorts=effective_sorts,
        order_key_spec=tuple(key_spec),
        use_parallel=use_parallel,
        count_policy=effective_search_count(leg_options),
        execution=execution,  # type: ignore[arg-type]
    )
