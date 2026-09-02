from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Final, Literal, TypeAlias, get_args, overload

import attrs

from forze.base.exceptions import exc

# ----------------------- #
# Pagination — generic page value objects with no search-specific metadata. The
# result-level facets / highlights / snapshot handle live on the SearchPage family
# in the search contract (forze.application.contracts.search.pages), which extends
# these; the base contract must not depend on the search contract.


AbstentionReason: TypeAlias = Literal["no_match", "ambiguous", "not_permitted"]
"""Why an empty page is empty, when the adapter can tell:

- ``no_match`` — nothing in the store/index matches the query;
- ``ambiguous`` — the query resolved to more than one candidate and the surface refuses
  to pick one;
- ``not_permitted`` — matching rows exist, but the caller's permissions filtered all of
  them out.

Abstention is a *result*, not an error: infrastructure failures stay typed exceptions.
The reason is optional — an adapter that cannot distinguish the causes returns an empty
page with no reason, which is what every adapter did before the field existed."""


_ABSTENTION_REASONS: Final[frozenset[str]] = frozenset(get_args(AbstentionReason))


def _validate_abstention(page: CountlessPage[Any] | CursorPage[Any]) -> None:
    if page.abstention is None:
        return

    # The vocabulary is closed at runtime too, not just for the type checker: a page is
    # built by adapter code whose reason strings no annotation reaches.
    if page.abstention not in _ABSTENTION_REASONS:
        raise exc.internal(
            f"Unknown abstention reason {page.abstention!r}; "
            f"expected one of {sorted(_ABSTENTION_REASONS)}.",
        )

    if page.hits:
        raise exc.internal(
            "An abstention reason is only valid on an empty page "
            f"(abstention={page.abstention!r}, hits={len(page.hits)}).",
        )


@attrs.define(slots=True, kw_only=True, frozen=True)
class CountlessPage[T]:
    """Value object for pagination result without a total count."""

    hits: list[T]
    """Items for the current page."""

    page: int
    """One-based page number."""

    size: int
    """Page size (number of records per page)."""

    abstention: AbstentionReason | None = None
    """Optional reason an empty page is empty; see :data:`AbstentionReason`. Never set on
    a page that carries hits."""

    def __attrs_post_init__(self) -> None:
        _validate_abstention(self)


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

    abstention: AbstentionReason | None = None
    """Optional reason an empty page is empty; see :data:`AbstentionReason`. Never set on
    a page that carries hits."""

    def __attrs_post_init__(self) -> None:
        _validate_abstention(self)


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
    abstention: AbstentionReason | None = None,
) -> CountlessPage[T]: ...


@overload
def page_from_limit_offset[T](
    hits: list[T],
    pagination: Mapping[str, Any] | None,
    *,
    total: int,
    abstention: AbstentionReason | None = None,
) -> Page[T]: ...


def page_from_limit_offset[T](
    hits: list[T],
    pagination: Mapping[str, Any] | None,
    *,
    total: int | None = None,
    abstention: AbstentionReason | None = None,
) -> Page[T] | CountlessPage[T]:
    """Build ``Page`` or ``CountlessPage`` from offset/limit window params.

    Used by adapters that run a single ``SELECT … LIMIT/OFFSET`` (no separate
    page number in the storage API). ``page`` is one-based: ``(offset // size) + 1``.
    """

    page_num, size = offset_page_coords(pagination, len(hits))

    if total is None:
        return CountlessPage(hits=hits, page=page_num, size=size, abstention=abstention)

    return Page(hits=hits, page=page_num, size=size, count=int(total), abstention=abstention)
