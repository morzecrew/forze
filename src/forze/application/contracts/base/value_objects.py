from __future__ import annotations

from typing import Any, Mapping

import attrs

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


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class Page[T](CountlessPage[T]):
    """Value object for pagination result with a total count."""

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


def page_from_limit_offset[T](
    hits: list[T],
    pagination: Mapping[str, Any] | None,
    *,
    total: int | None = None,
) -> Page[T] | CountlessPage[T]:
    """Build :class:`Page` or :class:`CountlessPage` from offset/limit window params.

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
        return CountlessPage(hits=hits, page=page_num, size=size)

    return Page(hits=hits, page=page_num, size=size, count=int(total))
