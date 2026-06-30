"""Windowed, memory-bounded build of a search-result snapshot pool.

Streams the ordered hit pool from a backend one ``chunk_size`` window at a time into the
snapshot store, so peak memory is a single window regardless of ``max_ids`` — instead of
fetching, decoding, and serializing the entire pool at once. Shared by every ranked offset
adapter (Meilisearch, Mongo, Postgres) and the PGroonga path.
"""

from typing import Any, Awaitable, Callable

import attrs

from forze.application.contracts.base import (
    FacetResults,
    HitHighlights,
    SearchSnapshotHandle,
)
from forze.application.contracts.search import (
    SearchResultSnapshotOptions,
    SearchResultSnapshotSpec,
)
from forze.base.primitives import JsonDict
from forze.base.serialization import ModelCodec

from .snapshot import SearchResultSnapshot

# ----------------------- #


@attrs.define(frozen=True, slots=True)
class SnapshotWindow:
    """One backend fetch window: ordered rows plus query-global extras (first window only)."""

    rows: list[JsonDict]
    facets: FacetResults | None = None
    highlights: list[HitHighlights] | None = None
    total: int | None = None


# ....................... #


@attrs.define(frozen=True, slots=True)
class SnapshotStreamResult:
    """Outcome of a streamed snapshot build: the handle plus the requested page slice."""

    handle: SearchSnapshotHandle
    page_rows: list[JsonDict]
    page_codec: ModelCodec[Any, Any]
    page_highlights: list[HitHighlights] | None
    facets: FacetResults | None
    total: int | None


# ....................... #


async def build_snapshot_pool_streaming(
    *,
    result_snapshot: SearchResultSnapshot,
    rs_spec: SearchResultSnapshotSpec,
    snap_opt: SearchResultSnapshotOptions | None,
    fp_computed: str,
    codec: ModelCodec[Any, Any],
    prepare_rows: (
        Callable[
            [list[JsonDict]], Awaitable[tuple[list[JsonDict], ModelCodec[Any, Any]]]
        ]
        | None
    ),
    fetch_window: Callable[[int, int], Awaitable[SnapshotWindow]],
    page_offset: int,
    page_limit: int,
    trust_source: bool = False,
) -> SnapshotStreamResult:
    """Stream the ordered pool window-by-window into a snapshot run, capturing the page slice.

    ``fetch_window(offset, limit)`` returns the next ordered window (and, on the first call,
    the query-global facets/total). ``prepare_rows`` decrypts a raw window and returns the
    codec to decode it with (``None`` decodes raw rows with *codec*, e.g. Postgres where the
    read codec itself decrypts). Only one window of rows and decoded models is alive at a time.
    """

    sink = result_snapshot.open_simple_hit_sink(
        snap_opt=snap_opt,
        rs_spec=rs_spec,
        fp_computed=fp_computed,
    )
    chunk = sink.chunk_size
    max_ids = sink.max_ids

    page_rows: list[JsonDict] = []
    page_highlights: list[HitHighlights] = []
    has_highlights = False
    facets: FacetResults | None = None
    total: int | None = None
    page_codec: ModelCodec[Any, Any] = codec

    seen = 0
    fetch_offset = 0
    page_end = page_offset + page_limit

    while seen < max_ids:
        want = min(chunk, max_ids - seen)
        window = await fetch_window(fetch_offset, want)

        if fetch_offset == 0:
            facets = window.facets
            total = window.total

        if not window.rows:
            break

        if prepare_rows is not None:
            rows, window_codec = await prepare_rows(window.rows)

        else:
            rows, window_codec = window.rows, codec

        if seen == 0:
            page_codec = window_codec

        models = window_codec.decode_mapping_many(rows, trust_source=trust_source)
        await sink.add(
            [SearchResultSnapshot.result_record_key_string(model) for model in models]
        )

        for i, row in enumerate(rows):
            global_index = seen + i

            if page_offset <= global_index < page_end:
                page_rows.append(row)

                # Highlights are per-hit and index-aligned with the page rows, so append one
                # entry for every page row when the window carries them — the hit's highlights
                # or ``{}`` when it has none — never skip, or later entries describe the wrong hit.
                if window.highlights is not None:
                    has_highlights = True
                    page_highlights.append(
                        window.highlights[i] if i < len(window.highlights) else {}
                    )

        seen += len(rows)
        fetch_offset += len(window.rows)

        # A short window means the backend has no more rows to give.
        if len(window.rows) < want:
            break

    handle = await sink.finish(pool_len_before_cap=seen)

    return SnapshotStreamResult(
        handle=handle,
        page_rows=page_rows,
        page_codec=page_codec,
        page_highlights=page_highlights if has_highlights else None,
        facets=facets,
        total=total,
    )
