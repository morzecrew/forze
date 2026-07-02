"""Late-materialized (thin) federated RRF offset execution.

Shared by every federated search adapter (Postgres, Meilisearch, mock). Instead of
fetching the full hits of the whole candidate union to fuse and sort — which holds
``members × rrf_per_leg_limit`` full hits in memory regardless of page size — this
fetches only ``id`` per leg, fuses on ``(member, id)``, and re-hydrates **just the
page's** full hits from each member. Peak memory becomes the thin candidate keys plus
one page of full hits, at the cost of one extra (page-sized) round trip per member.

When a result snapshot is configured, the thin path stores only ``(member, id)`` keys
(not full records), so the snapshot is tiny and the merge still never holds full hits;
replay (:meth:`SearchResultSnapshot.read_federated_thin_snapshot_page_if_requested`)
re-fetches the page's hits from the legs by id — the frozen order/identities replay with
**current** content, and a since-deleted hit drops out.

Gated by :attr:`~forze.application.contracts.search.FederatedSearchSpec.thin_merge`; the
adapters fall back to the full-fetch path otherwise (see :func:`federated_thin_eligible`).
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
    SearchResultSnapshotOptions,
    SearchResultSnapshotSpec,
    search_page_from_limit_offset,
)
from forze.base.primitives import MISSING, path_get
from forze.base.serialization import default_model_codec
from forze.domain.constants import ID_FIELD

from .snapshot import SearchResultSnapshot

# ----------------------- #

RunLegs = Callable[[Sequence[Callable[[], Awaitable[Any]]]], Awaitable[list[Any]]]
"""Runs leg thunks under a backend's concurrency rules and returns their results in order."""

# ....................... #


def federated_thin_format(
    members: Sequence[FederatedSearchMemberSpec],
    *,
    thin_merge: bool,
) -> bool:
    """Whether this spec's snapshots use the thin ``(member, id)`` format.

    Spec-level (not request-level): governs the snapshot key format, the replay read
    path, and the fingerprint marker. Requires the opt-in and that every member read
    model carries an ``id`` field (the fused identity and re-fetch key)."""

    return thin_merge and all(
        ID_FIELD in member.model_type.model_fields for member in members
    )


def federated_thin_eligible(
    *,
    members: Sequence[FederatedSearchMemberSpec],
    thin_merge: bool,
    wants_highlights: bool,
    sorts: QuerySortExpression | None,  # type: ignore[valid-type]
) -> bool:
    """Whether **this** fresh search can use the thin (id-only) merge path.

    Needs the thin snapshot format (:func:`federated_thin_format`). Highlights need the
    full leg hits up front, so they always fall back to the full-fetch path. Secondary
    ``sorts`` are supported thin by also projecting the sort fields, but only when every
    key is a top-level field present on all members: the full path reads sort values via
    ``getattr(hit, field)`` (no dotted traversal), so a dotted or member-missing key would
    order differently thin vs. full — those keep falling back."""

    if wants_highlights:
        return False

    if not federated_thin_format(members, thin_merge=thin_merge):
        return False

    if sorts:
        for field in sorts:
            if "." in field:
                return False
            if any(field not in member.model_type.model_fields for member in members):
                return False

    return True


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


async def _hydrate_federated_page(
    *,
    ports: dict[str, SearchQueryPort[Any]],
    ordered_keys: Sequence[tuple[str, str]],
    query: str | Sequence[str],
    filters: QueryFilterExpression | None,  # type: ignore[valid-type]
    leg_opts: SearchOptions | None,
    run_legs: RunLegs,
) -> list[FederatedSearchReadModel[Any]]:
    """Re-fetch full hits for ``ordered_keys`` (one query per member, by id), in order.

    A key whose hit is gone (deleted since the candidate fetch / snapshot write) is
    skipped. Used by both the fresh thin search and thin-snapshot replay.
    """

    ids_by_member: dict[str, list[str]] = {}

    for member, rid in ordered_keys:
        ids_by_member.setdefault(member, []).append(rid)

    members_in_order = list(ids_by_member.items())
    pages = await run_legs(
        [
            _hydrate(ports[member], query, filters, ids, leg_opts)
            for member, ids in members_in_order
        ]
    )

    hydrated: dict[tuple[str, str], BaseModel] = {}

    for (member, _ids), page in zip(members_in_order, pages, strict=True):
        for hit in page.hits:
            hydrated[(member, str(getattr(hit, ID_FIELD)))] = hit

    return [
        FederatedSearchReadModel(hit=hydrated[(member, rid)], member=member)
        for member, rid in ordered_keys
        if (member, rid) in hydrated
    ]


def federated_snapshot_rehydrator(
    *,
    ports: dict[str, SearchQueryPort[Any]],
    leg_opts: SearchOptions | None,
    run_legs: RunLegs,
) -> Callable[
    [Sequence[tuple[str, str]]], Awaitable[Sequence[FederatedSearchReadModel[Any]]]
]:
    """A re-fetch-by-id callback for thin-snapshot replay (current content, by id only).

    Replay re-fetches each key's hit by id (no query/filters: the snapshot froze the
    identities, not the matcher), so the returned content is current."""

    async def _rehydrate(
        ordered_keys: Sequence[tuple[str, str]],
    ) -> Sequence[FederatedSearchReadModel[Any]]:
        return await _hydrate_federated_page(
            ports=ports,
            ordered_keys=ordered_keys,
            query="",
            filters=None,
            leg_opts=leg_opts,
            run_legs=run_legs,
        )

    return _rehydrate


# ....................... #


async def execute_federated_thin_offset(
    *,
    legs: Sequence[tuple[str, SearchQueryPort[Any], float]],
    query: str | Sequence[str],
    filters: QueryFilterExpression | None,  # type: ignore[valid-type]
    pagination: PaginationExpression | None,
    sorts: QuerySortExpression | None = None,  # type: ignore[valid-type]
    leg_opts: SearchOptions | None,
    rrf_k: int,
    per_leg_limit: int,
    return_count: bool,
    return_type: type[BaseModel] | None,
    run_legs: RunLegs,
    result_snapshot: SearchResultSnapshot | None = None,
    rs_spec: SearchResultSnapshotSpec | None = None,
    snapshot: SearchResultSnapshotOptions | None = None,
    fp_computed: str = "",
    write_snapshot: bool = False,
) -> Any:
    """Thin RRF offset page: id-only fetch, fuse on ``(member, id)``, hydrate the page.

    *legs* are the active ``(member, port, weight)`` triples (member weight already
    applied upstream). *run_legs* runs the per-leg thunks under the backend's
    concurrency rules (pool-aware for Postgres, plain gather otherwise). *sorts* (when
    present) are projected alongside ``id`` per leg and applied to the fused set as
    stable tie-breakers under the RRF score — matching the full-fetch path exactly (the
    caller only routes eligible sorts here; see :func:`federated_thin_eligible`). When
    *write_snapshot*, the fused (and sorted) ``(member, id)`` keys are streamed into the
    snapshot store (tiny keys; replay re-fetches by id in the frozen order).
    """

    leg_page: PaginationExpression = {"limit": max(1, int(per_leg_limit))}
    sort_fields = tuple(sorts) if sorts else ()

    # 1. Thin candidate fetch: ``id`` (+ any sort fields) per leg, in relevance order.
    thin_pages = await run_legs(
        [
            _thin_fetch(port, query, filters, leg_page, leg_opts, sort_fields)
            for _name, port, _weight in legs
        ]
    )

    leg_rows: list[tuple[str, list[str], float]] = []
    values_by_key: dict[tuple[str, str], dict[str, Any]] = {}

    for (name, _port, weight), page in zip(legs, thin_pages, strict=True):
        ids: list[str] = []

        for row in page.hits:
            rid = str(row[ID_FIELD])
            ids.append(rid)

            if sort_fields:
                values_by_key[(name, rid)] = {
                    field: (None if (v := path_get(row, field)) is MISSING else v)
                    for field in sort_fields
                }

        leg_rows.append((name, ids, weight))

    # 2. Fuse on (member, id), then order (RRF score primary, sorts tie-break).
    merged = SearchResultSnapshot.weighted_rrf_merge_ids(leg_rows=leg_rows, k=rrf_k)
    total = len(merged)

    SearchResultSnapshot.order_federated_secondary_sorts(
        merged,
        sorts,
        value_of=lambda item, field: values_by_key[(item[0], item[1])][field],
        score_of=lambda item: -item[2],
    )

    # 2b. Snapshot write: stream the tiny (member, id) keys in final order (no full
    # records held); replay re-fetches by id and reproduces this order.
    handle = None

    if write_snapshot and result_snapshot is not None and rs_spec is not None:
        handle = await result_snapshot.put_ordered_snapshot_keys(
            (
                SearchResultSnapshot.federated_thin_record_key(member, rid)
                for member, rid, _score in merged
            ),
            snap_opt=snapshot,
            rs_spec=rs_spec,
            fp_computed=fp_computed,
            pool_len_before_cap=total,
        )

    # 3. Window to the requested page.
    offset = int((pagination or {}).get("offset") or 0)
    limit = (pagination or {}).get("limit")
    window = merged[offset:]

    if limit is not None:
        window = window[: int(limit)]

    # 4. Hydrate only the page: re-fetch full hits per member, restricted to its ids.
    ports = {name: port for name, port, _weight in legs}
    models = await _hydrate_federated_page(
        ports=ports,
        ordered_keys=[(member, rid) for member, rid, _score in window],
        query=query,
        filters=filters,
        leg_opts=leg_opts,
        run_legs=run_legs,
    )

    if return_type is not None:
        rows = [
            {"hit": fm.hit.model_dump(mode="json"), "member": fm.member}
            for fm in models
        ]
        hits: list[Any] = default_model_codec(return_type).decode_mapping_many(rows)

    else:
        hits = models

    return search_page_from_limit_offset(
        hits,
        pagination,
        total=total if return_count else None,
        snapshot=handle,
    )


# ....................... #


def _thin_fetch(
    port: SearchQueryPort[Any],
    query: str | Sequence[str],
    filters: QueryFilterExpression | None,  # type: ignore[valid-type]
    leg_page: PaginationExpression,
    leg_opts: SearchOptions | None,
    sort_fields: Sequence[str],
) -> Callable[[], Awaitable[Any]]:
    async def _run() -> Any:
        # Legs stay in relevance order (no ``sorts`` passed); the sort fields ride along
        # only so the merged set can be ordered by them after fusion.
        return await port.project_search(
            [ID_FIELD, *sort_fields], query, filters, leg_page, options=leg_opts
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
