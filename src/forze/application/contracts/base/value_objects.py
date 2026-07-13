from __future__ import annotations

from collections.abc import Mapping
from typing import Any, overload

import attrs

# ----------------------- #
# Pagination — generic page value objects with no search-specific metadata. The
# result-level facets / highlights / snapshot handle live on the SearchPage family
# in the search contract (forze.application.contracts.search.pages), which extends
# these; the base contract must not depend on the search contract.


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


def offset_page_coords(pagination: Mapping[str, Any] | None, hit_count: int) -> tuple[int, int]:
    """Resolve ``(page_number, size)`` from offset/limit window params (``page`` one-based).

    Shared by :func:`page_from_limit_offset` and the search page builder so both number a
    single ``SELECT … LIMIT/OFFSET`` window identically.
    """

    p = dict(pagination or {})
    limit = p.get("limit")
    offset = int(p.get("offset") or 0)

    # A missing/empty/zero limit takes the unlimited fallback (mirrors the ``offset or 0``
    # tolerance); only a positive limit casts, so ``""`` never reaches ``int()`` and raises.
    if not limit:
        return 1, (max(hit_count, 1) if hit_count else 1)

    size = max(int(limit), 1)
    return (offset // size) + 1, size


@overload
def page_from_limit_offset[T](
    hits: list[T],
    pagination: Mapping[str, Any] | None,
    *,
    total: None = None,
) -> CountlessPage[T]: ...


@overload
def page_from_limit_offset[T](
    hits: list[T],
    pagination: Mapping[str, Any] | None,
    *,
    total: int,
) -> Page[T]: ...


def page_from_limit_offset[T](
    hits: list[T],
    pagination: Mapping[str, Any] | None,
    *,
    total: int | None = None,
) -> Page[T] | CountlessPage[T]:
    """Build ``Page`` or ``CountlessPage`` from offset/limit window params.

    Used by adapters that run a single ``SELECT … LIMIT/OFFSET`` (no separate
    page number in the storage API). ``page`` is one-based: ``(offset // size) + 1``.
    """

    page_num, size = offset_page_coords(pagination, len(hits))

    if total is None:
        return CountlessPage(hits=hits, page=page_num, size=size)

    return Page(hits=hits, page=page_num, size=size, count=int(total))
