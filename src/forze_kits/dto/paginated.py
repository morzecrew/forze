"""Paginated request and response DTOs."""

from __future__ import annotations

from typing import Any, cast

from pydantic import BaseModel, PositiveInt

from forze.application.contracts.base import CursorPage, Page
from forze.application.contracts.querying import (
    CursorPaginationExpression,
    PaginationExpression,
)
from forze.base.primitives import JsonDict
from forze.domain.models import BaseDTO

# ----------------------- #


class Pagination(BaseDTO):
    """Pagination request payload."""

    page: PositiveInt = 1
    """One-based page number."""

    size: PositiveInt = 10
    """Page size (number of records per page)."""

    # ....................... #

    @property
    def offset_limit(self) -> tuple[int, int]:
        """Return ``(limit, offset)`` for the requested one-based page."""

        return self.size, (self.page - 1) * self.size

    # ....................... #

    def to_offset_expression(self) -> PaginationExpression:
        """Offset/limit pagination expression for the requested page."""

        limit, offset = self.offset_limit

        return {"limit": limit, "offset": offset}


# ....................... #


class CursorPagination(BaseDTO):
    """Cursor pagination request payload."""

    size: PositiveInt = 10
    """Page size. Adapters may apply a default when omitted."""

    after: str | None = None
    """Opaque token from a prior response's ``next_cursor`` (forward)."""

    before: str | None = None
    """Opaque token from a prior response's ``prev_cursor`` (backward)"""

    # ....................... #

    def to_cursor_expression(self) -> CursorPaginationExpression:
        c: CursorPaginationExpression = {}

        c["limit"] = self.size

        if self.after is not None:
            c["after"] = self.after

        if self.before is not None:
            c["before"] = self.before

        return c


# ....................... #


def offset_page_fields(page: Page[Any]) -> dict[str, Any]:
    """Shared response fields for offset ``from_page`` builders."""

    return {
        "hits": page.hits,
        "page": page.page,
        "size": page.size,
        "count": page.count,
    }


# ....................... #


def cursor_page_fields(page: CursorPage[Any]) -> dict[str, Any]:
    """Shared response fields for cursor ``from_page`` builders."""

    return {
        "hits": page.hits,
        "next_cursor": page.next_cursor,
        "prev_cursor": page.prev_cursor,
        "has_more": page.has_more,
    }


# ....................... #


class Paginated[T: BaseModel](BaseDTO):
    """Paginated response with typed hit records.

    Used when search returns domain read models (e.g. `ReadDocument`).
    `page` and `size` describe the requested slice; `count` is the
    total number of matching records.
    """

    hits: list[T]
    """Records for the current page."""

    page: int
    """One-based page number."""

    size: int
    """Page size (number of records per page)."""

    count: int
    """Total number of matching records across all pages."""

    # ....................... #

    @classmethod
    def from_page[X: BaseModel](cls, page: Page[X]) -> Paginated[X]:
        out = cast(type[Paginated[X]], cls)

        return out(**offset_page_fields(page))


# ....................... #


class ProjectedPaginated(BaseDTO):
    """Paginated response with raw dict hit records.

    Used when search returns field-projected JSON mappings instead of typed
    models. Same pagination semantics as `Paginated`.
    """

    hits: list[JsonDict]
    """Raw record dicts for the current page."""

    page: int
    """One-based page number."""

    size: int
    """Page size (number of records per page)."""

    count: int
    """Total number of matching records across all pages."""

    # ....................... #

    @classmethod
    def from_page(cls, page: Page[JsonDict]) -> ProjectedPaginated:
        return cls(**offset_page_fields(page))


# ....................... #


class CursorPaginated[T: BaseModel](BaseDTO):
    """Cursor-paginated response with typed hit records."""

    hits: list[T]
    """Records for the current page."""

    next_cursor: str | None
    """Opaque token for the next page, or ``None`` if this is the last page."""

    prev_cursor: str | None
    """Opaque token for the previous page, or ``None`` if this is the first page."""

    has_more: bool = False
    """Whether there are more pages after this one."""

    # ....................... #

    @classmethod
    def from_page[X: BaseModel](cls, page: CursorPage[X]) -> CursorPaginated[X]:
        out = cast(type[CursorPaginated[X]], cls)

        return out(**cursor_page_fields(page))


# ....................... #


class ProjectedCursorPaginated(BaseDTO):
    """Cursor-paginated response with raw dict hit records."""

    hits: list[JsonDict]
    """Raw record dicts for the current page."""

    next_cursor: str | None
    """Opaque token for the next page, or ``None`` if this is the last page."""

    prev_cursor: str | None
    """Opaque token for the previous page, or ``None`` if this is the first page."""

    has_more: bool = False
    """Whether there are more pages after this one."""

    # ....................... #

    @classmethod
    def from_page(cls, page: CursorPage[JsonDict]) -> ProjectedCursorPaginated:
        return cls(**cursor_page_fields(page))
