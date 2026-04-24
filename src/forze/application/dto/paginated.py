"""Paginated response DTOs for search and list operations.

Provides :class:`Paginated` (typed hits) and :class:`RawPaginated` (raw dict
hits). Page numbers are one-based; ``count`` is the total across all pages.
"""

from __future__ import annotations

from typing import cast

from pydantic import BaseModel, PositiveInt

from forze.application.contracts.base import CursorPage, Page, SearchSnapshotHandle
from forze.application.contracts.query import CursorPaginationExpression
from forze.base.primitives import JsonDict
from forze.domain.models import BaseDTO

# ----------------------- #


class SearchSnapshotHandleDTO(BaseDTO):
    """Thin response DTO for :class:`~forze.application.contracts.base.SearchSnapshotHandle`.

    Echo ``id`` and ``fingerprint`` in the next request under
    ``SearchOptions`` ``result_snapshot`` to continue from the KV snapshot.
    """

    id: str
    """Snapshot run id."""

    fingerprint: str
    """Request fingerprint; echo for :meth:`~forze.application.contracts.search.SearchResultSnapshotPort.get_id_range`."""

    total: int
    """Number of entries materialized in the snapshot (after cap)."""

    capped: bool = False
    """Whether the result set was truncated to ``max_ids`` when the snapshot was written."""

    # ....................... #

    @classmethod
    def from_handle(
        cls,
        handle: SearchSnapshotHandle | None,
    ) -> SearchSnapshotHandleDTO | None:
        """Map a contract handle to DTO, or ``None``."""

        if handle is None:
            return None

        return cls(
            id=handle.id,
            fingerprint=handle.fingerprint,
            total=handle.total,
            capped=handle.capped,
        )


# ....................... #


class Pagination(BaseDTO):
    """Pagination request payload."""

    page: PositiveInt = 1
    """One-based page number."""

    size: PositiveInt = 10
    """Page size (number of records per page)."""


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

    result_snapshot: SearchSnapshotHandleDTO | None = None
    """When present, KV result snapshot metadata for paged follow-up (send back in request ``options``)."""

    # ....................... #

    @classmethod
    def from_page[X: BaseModel](cls, page: Page[X]) -> Paginated[X]:
        out = cast(type[Paginated[X]], cls)

        return out(
            hits=page.hits,
            page=page.page,
            size=page.size,
            count=page.count,
            result_snapshot=SearchSnapshotHandleDTO.from_handle(page.result_snapshot),
        )


# ....................... #


class RawPaginated(BaseDTO):
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

    result_snapshot: SearchSnapshotHandleDTO | None = None
    """When present, KV result snapshot metadata for paged follow-up (send back in request ``options``)."""

    # ....................... #

    @classmethod
    def from_page(cls, page: Page[JsonDict]) -> RawPaginated:
        return cls(
            hits=page.hits,
            page=page.page,
            size=page.size,
            count=page.count,
            result_snapshot=SearchSnapshotHandleDTO.from_handle(page.result_snapshot),
        )


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

        return out(
            hits=page.hits,
            next_cursor=page.next_cursor,
            prev_cursor=page.prev_cursor,
            has_more=page.has_more,
        )


# ....................... #


class RawCursorPaginated(BaseDTO):
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
    def from_page(cls, page: CursorPage[JsonDict]) -> RawCursorPaginated:
        return cls(
            hits=page.hits,
            next_cursor=page.next_cursor,
            prev_cursor=page.prev_cursor,
            has_more=page.has_more,
        )
