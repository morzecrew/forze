"""Backend-agnostic orchestration for simple offset search with optional snapshots."""

from typing import Any, Protocol, Sequence, TypeVar, runtime_checkable

import attrs
from pydantic import BaseModel

from forze.application.contracts.base import (
    FacetResults,
    HitHighlights,
    page_from_limit_offset,
)
from forze.application.contracts.querying import (
    PaginationExpression,
    QueryFilterExpression,
    QuerySortExpression,
)
from forze.application.contracts.search import (
    SearchResultSnapshotOptions,
    SearchSpec,
)
from forze.application.integrations.search.encryption import (
    decrypt_search_rows,
    reject_encrypted_sort_fields,
)
from forze.application.integrations.search._snapshot_stream import (
    SnapshotWindow,
    build_snapshot_pool_streaming,
)
from forze.application.integrations.search.snapshot import SearchResultSnapshot
from forze.base.primitives import JsonDict
from forze.base.serialization import ModelCodec, materialize_mapping_rows
from collections.abc import Awaitable

# ----------------------- #

M = TypeVar("M", bound=BaseModel)

# ....................... #


def offset_from_dict(pagination_dict: JsonDict) -> int:
    """Read ``offset`` from a pagination mapping (``dict(pagination)``)."""

    raw = pagination_dict.get("offset")

    if raw is None:
        return 0

    return int(raw)


# ....................... #


@attrs.define(frozen=True, slots=True)
class OffsetFetchWindow:
    """Pagination window passed to backend fetch hooks."""

    fetch_limit: int | None
    """Backend ``LIMIT`` (``None`` when unlimited)."""

    fetch_offset: int
    """Backend ``OFFSET`` (0 when snapshot pool fetch starts at top)."""

    page_offset: int
    """User page offset used to slice the snapshot pool."""

    page_limit: int
    """User page size."""


# ....................... #


@attrs.define(frozen=True, slots=True)
class OffsetRowsResult:
    """Rows returned from a backend fetch."""

    rows: list[JsonDict]
    """Ordered hit rows (storage-shaped dicts)."""

    total: int | None = None
    """When set and counting is enabled, used as the result total."""

    facets: FacetResults | None = None
    """Optional facet distributions for this search (result-level; see RFC 0006)."""

    highlights: list[HitHighlights] | None = None
    """Optional per-hit highlighted fragments, index-aligned with :attr:`rows`
    (sliced in lockstep with the rows when a snapshot pool is paginated)."""


# ....................... #


@runtime_checkable
class SimpleOffsetSearchHooks(Protocol):
    """Backend callbacks for one offset search miss (after snapshot read)."""

    def fetch_count(self) -> Awaitable[int | None]:
        """Return total when counting; ``None`` if count is deferred to :meth:`fetch_rows`."""
        ...

    def fetch_rows(
        self,
        window: OffsetFetchWindow,
        *,
        want_snap: bool,
    ) -> Awaitable[OffsetRowsResult]:
        """Fetch ordered rows for the given window."""
        ...


# ....................... #


async def execute_simple_offset_search_with_snapshot[M: BaseModel](
    *,
    query: str | Sequence[str],
    filters: QueryFilterExpression | None,  # type: ignore[valid-type]
    sorts: QuerySortExpression | None,  # type: ignore[valid-type]
    fingerprint_sorts: QuerySortExpression | None = None,  # type: ignore[valid-type]
    spec: SearchSpec[Any],
    variant: str,
    fingerprint_extras: dict[str, object] | None,
    pagination: PaginationExpression | None,
    snapshot: SearchResultSnapshotOptions | None,
    return_count: bool,
    snapshot_return_count: bool | None = None,
    page_return_count: bool | None = None,
    return_type: type[BaseModel] | None,
    return_fields: Sequence[str] | None,
    model_type: type[M],
    codec: ModelCodec[Any, Any],
    result_snapshot: SearchResultSnapshot | None,
    hooks: SimpleOffsetSearchHooks,
    trust_source: bool = False,
) -> Any:
    """Run fingerprint, snapshot read, count, fetch, materialize, and page wrapping."""

    rs_spec = spec.snapshot

    # The fingerprint is only consumed on the snapshot read/write paths below, which
    # require both a snapshot port and a snapshot spec. Skip hashing the whole filter
    # (orjson dump + SHA-256) entirely when snapshots are not configured.
    snapshots_enabled = result_snapshot is not None and rs_spec is not None

    if snapshots_enabled:
        fp_sorts = fingerprint_sorts if fingerprint_sorts is not None else sorts
        fp_fingerprint = SearchResultSnapshot.simple_search_fingerprint(
            query,
            filters,
            fp_sorts,
            spec_name=spec.name,
            variant=variant,
            extras=fingerprint_extras,
        )

    else:
        fp_fingerprint = ""

    pagination_dict: dict[str, Any] = dict(pagination or {})
    snap_return_count = (
        return_count if snapshot_return_count is None else snapshot_return_count
    )
    emit_total = return_count if page_return_count is None else page_return_count

    if result_snapshot is not None and rs_spec is not None:
        maybe_snap: Any = await result_snapshot.read_simple_result_snapshot(
            rs_spec=rs_spec,
            snap_opt=snapshot,
            fp_computed=fp_fingerprint,
            spec=spec,
            pagination=pagination_dict,
            return_type=return_type,
            return_fields=return_fields,
            return_count=snap_return_count,
        )

        if maybe_snap is not None:
            return maybe_snap

    # Fall-through to a real backend fetch (replay above did not serve the page). Only now
    # fail closed on a sort over a field-encrypted column: encrypted/searchable ciphertext
    # has no order at rest and would leak the raw value into a keyset cursor token. Deferred
    # to here so paging an existing snapshot by id (which never re-sorts rows) still works —
    # including snapshots created before this guard existed.
    reject_encrypted_sort_fields(sorts, encryption=spec.encryption, spec_name=spec.name)

    total: int | None = None

    if return_count:
        total = await hooks.fetch_count()

        if total is not None and total == 0:
            return page_from_limit_offset(  # pyright: ignore[reportUnknownVariableType]
                [],
                pagination_dict,
                total=0,
            )

    want_snap = (
        result_snapshot is not None
        and rs_spec is not None
        and result_snapshot.should_write_result_snapshot(snapshot, rs_spec)
    )
    page_offset = offset_from_dict(pagination_dict)

    if want_snap and result_snapshot is not None and rs_spec is not None:
        # Snapshot write: stream the ordered pool window-by-window straight into the store so
        # peak memory is one chunk, never the whole (up to ``max_ids``) decoded pool at once.
        page_limit = SearchResultSnapshot.snapshot_pagination(
            True, 0, pagination_dict
        )[2]
        read_codec = codec

        async def fetch_window(
            window_offset: int, window_limit: int
        ) -> SnapshotWindow:
            outcome = await hooks.fetch_rows(
                OffsetFetchWindow(
                    fetch_limit=window_limit,
                    fetch_offset=window_offset,
                    page_offset=page_offset,
                    page_limit=page_limit,
                ),
                want_snap=True,
            )

            return SnapshotWindow(
                rows=outcome.rows,
                facets=outcome.facets,
                highlights=outcome.highlights,
                total=outcome.total,
            )

        async def prepare_window_rows(
            raw_rows: list[JsonDict],
        ) -> tuple[list[JsonDict], ModelCodec[Any, Any]]:
            return await decrypt_search_rows(read_codec, raw_rows)

        stream = await build_snapshot_pool_streaming(
            result_snapshot=result_snapshot,
            rs_spec=rs_spec,
            snap_opt=snapshot,
            fp_computed=fp_fingerprint,
            codec=read_codec,
            prepare_rows=prepare_window_rows,
            fetch_window=fetch_window,
            page_offset=page_offset,
            page_limit=page_limit,
            trust_source=trust_source,
        )

        if return_count and total is None:
            total = stream.total

        page = materialize_mapping_rows(
            codec=stream.page_codec,
            model_type=model_type,
            page_rows=stream.page_rows,
            pool=None,
            u=page_offset,
            page_limit=page_limit,
            return_type=return_type,
            return_fields=return_fields,
            trust_source=trust_source,
        )

        snap_result: Any = page_from_limit_offset(
            page,
            pagination_dict,
            total=total if emit_total else None,
            snapshot=stream.handle,
        )

        if stream.facets is None and stream.page_highlights is None:
            return snap_result

        return attrs.evolve(
            snap_result, facets=stream.facets, highlights=stream.page_highlights
        )

    fetch_limit, fetch_offset, page_limit = SearchResultSnapshot.snapshot_pagination(
        False,
        0,
        pagination_dict,
    )

    window = OffsetFetchWindow(
        fetch_limit=fetch_limit,
        fetch_offset=fetch_offset,
        page_offset=page_offset,
        page_limit=page_limit,
    )

    outcome = await hooks.fetch_rows(window, want_snap=False)
    rows, codec = await decrypt_search_rows(codec, outcome.rows)

    if return_count and total is None and outcome.total is not None:
        total = outcome.total

        if total == 0:
            return page_from_limit_offset(  # pyright: ignore[reportUnknownVariableType]
                [],
                pagination_dict,
                total=0,
            )

    return await snapshot_materialize_and_paginate(
        rows=rows,
        want_snap=False,
        result_snapshot=result_snapshot,
        rs_spec=rs_spec,
        snapshot=snapshot,
        fp_fingerprint=fp_fingerprint,
        pagination_dict=pagination_dict,
        page_limit=page_limit,
        return_count=emit_total,
        total=total,
        return_type=return_type,
        return_fields=return_fields,
        model_type=model_type,
        codec=codec,
        trust_source=trust_source,
        facets=outcome.facets,
        highlights=outcome.highlights,
    )


# ....................... #


async def snapshot_materialize_and_paginate[M: BaseModel](
    *,
    rows: list[JsonDict],
    want_snap: bool,
    result_snapshot: SearchResultSnapshot | None,
    rs_spec: Any,
    snapshot: SearchResultSnapshotOptions | None,
    fp_fingerprint: str,
    pagination_dict: dict[str, Any],
    page_limit: int,
    return_count: bool,
    total: int | None,
    return_type: type[BaseModel] | None,
    return_fields: Sequence[str] | None,
    model_type: type[M],
    codec: ModelCodec[Any, Any],
    trust_source: bool = False,
    facets: FacetResults | None = None,
    highlights: list[HitHighlights] | None = None,
) -> Any:
    """Snapshot write, in-memory slice, materialize, and :func:`page_from_limit_offset`."""

    handle_out = None
    pool_snap: list[M] | None = None
    page_offset = offset_from_dict(pagination_dict)

    if want_snap and result_snapshot is not None and rs_spec is not None:
        pool_len = len(rows)
        pool_snap = codec.decode_mapping_many(rows, trust_source=trust_source)
        handle_out = await result_snapshot.put_simple_ordered_hits(
            pool_snap,
            snap_opt=snapshot,
            rs_spec=rs_spec,
            fp_computed=fp_fingerprint,
            pool_len_before_cap=pool_len,
        )
        rows = rows[page_offset : page_offset + page_limit]
        # Keep per-hit highlights aligned with the page slice of the pooled rows.
        if highlights is not None:
            highlights = highlights[page_offset : page_offset + page_limit]

    effective_page_limit = (
        page_limit
        if want_snap
        else (
            int(pagination_dict["limit"])
            if pagination_dict.get("limit") is not None
            else len(rows)
        )
    )

    page = materialize_mapping_rows(
        codec=codec,
        model_type=model_type,
        page_rows=rows,
        pool=pool_snap,
        u=page_offset,
        page_limit=effective_page_limit,
        return_type=return_type,
        return_fields=return_fields,
        trust_source=trust_source,
    )

    result: Any = page_from_limit_offset(
        page,
        pagination_dict,
        total=total if return_count else None,
        snapshot=handle_out,
    )

    if facets is None and highlights is None:
        return result

    return attrs.evolve(result, facets=facets, highlights=highlights)
