"""Late-materialized (thin) federated RRF offset execution.

Shared by every federated search adapter (Postgres, Meilisearch, mock). Instead of
fetching the full hits of the whole candidate union to fuse and sort — which holds
``members × rrf_per_leg_limit`` full hits in memory regardless of page size — this
fetches only ``id`` per leg, fuses on ``(member, id)``, and re-hydrates **just the
page's** full hits from each member. Peak memory becomes the thin candidate keys plus
one page of full hits, at the cost of one extra (page-sized) round trip per member.

Gated by :attr:`~forze.application.contracts.search.FederatedSearchSpec.thin_merge` and
the eligibility check in :func:`federated_thin_eligible`; the adapters fall back to the
full-fetch path otherwise.
"""

from __future__ import annotations

from typing import Any, Awaitable, Callable, Sequence

from pydantic import BaseModel

from forze.application.contracts.querying import (
    PaginationExpression,
    QueryFilterExpression,
    QuerySortExpression,
)
from forze.application.contracts.search import (
    FederatedSearchMemberSpec,
    FederatedSearchReadModel,
    SearchOptions,
    SearchQueryPort,
    search_page_from_limit_offset,
)
from forze.base.serialization import default_model_codec
from forze.domain.constants import ID_FIELD

from .snapshot import SearchResultSnapshot

# ----------------------- #

RunLegs = Callable[[Sequence[Callable[[], Awaitable[Any]]]], Awaitable[list[Any]]]
"""Runs leg thunks under a backend's concurrency rules and returns their results in order."""

# ....................... #


def federated_thin_eligible(
    *,
    members: Sequence[FederatedSearchMemberSpec],
    thin_merge: bool,
    wants_highlights: bool,
    sorts: QuerySortExpression | None,  # type: ignore[valid-type]
    snapshot_write: bool,
) -> bool:
    """Whether a federated search can use the thin (id-only) merge path.

    Requires the spec opt-in and a search that needs neither the full leg hits up
    front (highlights, or a secondary ``sorts`` over hit fields) nor a result-snapshot
    write (the snapshot stores full records for leg-free replay), and whose every
    member read model carries an ``id`` field (the fused identity / re-fetch key).
    """

    if not thin_merge or wants_highlights or sorts or snapshot_write:
        return False

    return all(ID_FIELD in member.model_type.model_fields for member in members)


# ....................... #


def _and_id_filter(
    filters: QueryFilterExpression | None,  # type: ignore[valid-type]
    ids: Sequence[str],
) -> QueryFilterExpression:  # type: ignore[valid-type]
    """``filters AND id IN ids`` — the page-hydration restriction."""

    id_clause: Any = {"$values": {ID_FIELD: {"$in": list(ids)}}}

    if filters is None:
        return id_clause

    return {"$and": [filters, id_clause]}


# ....................... #


async def execute_federated_thin_offset(
    *,
    legs: Sequence[tuple[str, SearchQueryPort[Any], float]],
    query: str | Sequence[str],
    filters: QueryFilterExpression | None,  # type: ignore[valid-type]
    pagination: PaginationExpression | None,
    leg_opts: SearchOptions | None,
    rrf_k: int,
    per_leg_limit: int,
    return_count: bool,
    return_type: type[BaseModel] | None,
    run_legs: RunLegs,
) -> Any:
    """Thin RRF offset page: id-only fetch, fuse on ``(member, id)``, hydrate the page.

    *legs* are the active ``(member, port, weight)`` triples (member weight already
    applied upstream). *run_legs* runs the per-leg thunks under the backend's
    concurrency rules (pool-aware for Postgres, plain gather otherwise).
    """

    leg_page: PaginationExpression = {"limit": max(1, int(per_leg_limit))}

    # 1. Thin candidate fetch: only ``id`` per leg, kept in each leg's relevance order.
    thin_pages = await run_legs(
        [
            _thin_fetch(port, query, filters, leg_page, leg_opts)
            for _name, port, _weight in legs
        ]
    )
    leg_rows = [
        (name, [str(row[ID_FIELD]) for row in page.hits], weight)
        for (name, _port, weight), page in zip(legs, thin_pages, strict=True)
    ]

    # 2. Fuse on (member, id); 3. window to the requested page.
    merged = SearchResultSnapshot.weighted_rrf_merge_ids(leg_rows=leg_rows, k=rrf_k)
    total = len(merged)

    offset = int((pagination or {}).get("offset") or 0)
    limit = (pagination or {}).get("limit")
    window = merged[offset:]

    if limit is not None:
        window = window[: int(limit)]

    # 4. Hydrate only the page: re-fetch full hits per member, restricted to its ids.
    ports = {name: port for name, port, _weight in legs}
    ids_by_member: dict[str, list[str]] = {}

    for member, rid, _score in window:
        ids_by_member.setdefault(member, []).append(rid)

    members_in_order = list(ids_by_member.items())
    hydrated_pages = await run_legs(
        [
            _hydrate(ports[member], query, filters, ids, leg_opts)
            for member, ids in members_in_order
        ]
    )

    hydrated: dict[tuple[str, str], BaseModel] = {}

    for (member, _ids), page in zip(members_in_order, hydrated_pages, strict=True):
        for hit in page.hits:
            hydrated[(member, str(getattr(hit, ID_FIELD)))] = hit

    # 5. Reassemble in fused order (a hit deleted between fetch and hydrate is skipped).
    models = [
        FederatedSearchReadModel(hit=hydrated[(member, rid)], member=member)
        for member, rid, _score in window
        if (member, rid) in hydrated
    ]

    if return_type is not None:
        rows = [
            {"hit": fm.hit.model_dump(mode="json"), "member": fm.member}
            for fm in models
        ]
        hits: list[Any] = default_model_codec(return_type).decode_mapping_many(rows)

    else:
        hits = models

    # No snapshot / highlights / facets on this path (excluded by eligibility).
    return search_page_from_limit_offset(
        hits,
        pagination,
        total=total if return_count else None,
    )


# ....................... #


def _thin_fetch(
    port: SearchQueryPort[Any],
    query: str | Sequence[str],
    filters: QueryFilterExpression | None,  # type: ignore[valid-type]
    leg_page: PaginationExpression,
    leg_opts: SearchOptions | None,
) -> Callable[[], Awaitable[Any]]:
    async def _run() -> Any:
        return await port.project_search(
            [ID_FIELD], query, filters, leg_page, options=leg_opts
        )

    return _run


def _hydrate(
    port: SearchQueryPort[Any],
    query: str | Sequence[str],
    filters: QueryFilterExpression | None,  # type: ignore[valid-type]
    ids: Sequence[str],
    leg_opts: SearchOptions | None,
) -> Callable[[], Awaitable[Any]]:
    async def _run() -> Any:
        return await port.search(
            query,
            _and_id_filter(filters, ids),
            {"limit": len(ids)},
            options=leg_opts,
        )

    return _run
