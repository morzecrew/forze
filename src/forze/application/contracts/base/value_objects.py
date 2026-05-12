from __future__ import annotations

from typing import Any, Mapping, overload

import attrs

# ----------------------- #
# Search snapshot (optional metadata on paged search responses)


@attrs.define(slots=True, kw_only=True, frozen=True)
class SearchSnapshotHandle:
    """Opaque handle to continue paged search without re-running the full query (KV snapshot)."""

    id: str
    """Snapshot run id; send back as ``forze.application.contracts.search.types.SearchResultSnapshotOptions.id``."""

    fingerprint: str
    """Stable request fingerprint; clients should echo for validation."""

    total: int
    """Number of entries materialized in the snapshot (after cap)."""

    capped: bool = False
    """``True`` if the result set was truncated to ``max_ids`` when the snapshot was written."""


# ----------------------- #
# Pagination


@attrs.define(slots=True, kw_only=True, frozen=True)
class CountlessPage[T]:
    """Value object for pagination result without a total count."""

    hits: list[T]
    """Items for the current page."""

    page: int
    """One-based page number."""

    size: int
    """Page size (number of records per page)."""

    snapshot: SearchSnapshotHandle | None = None
    """When present, a snapshot of ordered ids was used or created for this search."""


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class Page[T](CountlessPage[T]):
    """Value object for pagination result with a total count.

    Inherits optional :attr:`CountlessPage.result_snapshot` for search snapshotting.
    """

    count: int
    """Total number of matching records across all pages."""


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class CursorPage[T]:
    """Value object for cursor pagination result without a total count."""

    hits: list[T]
    """Items for the current page."""

    next_cursor: str | None
    """Opaque token for the next page, or ``None`` if this is the last page."""

    prev_cursor: str | None
    """Opaque token for the previous page, or ``None`` if this is the first page."""

    has_more: bool = False
    """Whether there are more pages after this one."""


# ....................... #


@overload
def page_from_limit_offset[T](
    hits: list[T],
    pagination: Mapping[str, Any] | None,
    *,
    total: None = None,
    snapshot: SearchSnapshotHandle | None = None,
) -> CountlessPage[T]: ...


@overload
def page_from_limit_offset[T](
    hits: list[T],
    pagination: Mapping[str, Any] | None,
    *,
    total: int,
    snapshot: SearchSnapshotHandle | None = None,
) -> Page[T]: ...


def page_from_limit_offset[T](
    hits: list[T],
    pagination: Mapping[str, Any] | None,
    *,
    total: int | None = None,
    snapshot: SearchSnapshotHandle | None = None,
) -> Page[T] | CountlessPage[T]:
    """Build ``Page`` or ``CountlessPage`` from offset/limit window params.

    Used by adapters that run a single ``SELECT … LIMIT/OFFSET`` (no separate
    page number in the storage API). ``page`` is one-based: ``(offset // size) + 1``.
    """

    p = dict(pagination or {})
    limit = p.get("limit")
    offset = int(p.get("offset") or 0)

    if limit is None:
        size = max(len(hits), 1) if hits else 1
        page_num = 1

    else:
        size = max(int(limit), 1)
        page_num = (offset // size) + 1

    if total is None:
        return CountlessPage(
            hits=hits,
            page=page_num,
            size=size,
            snapshot=snapshot,
        )

    return Page(
        hits=hits,
        page=page_num,
        size=size,
        count=int(total),
        snapshot=snapshot,
    )
