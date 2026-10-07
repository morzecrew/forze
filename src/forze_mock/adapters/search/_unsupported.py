"""Offset-only mock search adapters reject cursor/projection entrypoints."""

from __future__ import annotations

from collections.abc import AsyncGenerator, Sequence
from typing import Generic, NoReturn, TypeVar

from pydantic import BaseModel

from forze.application.contracts.base import CountlessPage, Page
from forze.application.contracts.querying import (
    AggregatesExpression,
    CursorPaginationExpression,
    PaginationExpression,
    QueryFilterExpression,
    QuerySortExpression,
)
from forze.application.contracts.search import (
    DEFAULT_SEARCH_CAPABILITIES,
    SearchCountlessPage,
    SearchCursorPage,
    SearchOptions,
    SearchPage,
    SearchResultSnapshotOptions,
    validate_aggregates_supported,
)
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict

# ----------------------- #

T = TypeVar("T", bound=BaseModel)
M = TypeVar("M", bound=BaseModel)


def _unsupported_cursor() -> NoReturn:
    raise exc.precondition("Mock hub/federated search supports offset pagination only")


class MockOffsetOnlySearchMixin(Generic[M]):
    """Shared ``NotImplemented`` surface for simplified mock search adapters."""

    async def search_cursor(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,
        cursor: CursorPaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
    ) -> SearchCursorPage[M]:
        _ = query, filters, cursor, sorts, options
        _unsupported_cursor()

    async def project_search(
        self,
        fields: Sequence[str],
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        snapshot: SearchResultSnapshotOptions | None = None,
    ) -> SearchCountlessPage[JsonDict]:
        _ = fields, query, filters, pagination, sorts, options, snapshot
        _unsupported_cursor()

    async def project_search_page(
        self,
        fields: Sequence[str],
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        snapshot: SearchResultSnapshotOptions | None = None,
    ) -> SearchPage[JsonDict]:
        _ = fields, query, filters, pagination, sorts, options, snapshot
        _unsupported_cursor()

    async def project_search_cursor(
        self,
        fields: Sequence[str],
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,
        cursor: CursorPaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
    ) -> SearchCursorPage[JsonDict]:
        _ = fields, query, filters, cursor, sorts, options
        _unsupported_cursor()

    async def select_search(
        self,
        return_type: type[T],
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        snapshot: SearchResultSnapshotOptions | None = None,
    ) -> SearchCountlessPage[T]:
        _ = return_type, query, filters, pagination, sorts, options, snapshot
        _unsupported_cursor()

    async def select_search_page(
        self,
        return_type: type[T],
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        snapshot: SearchResultSnapshotOptions | None = None,
    ) -> SearchPage[T]:
        _ = return_type, query, filters, pagination, sorts, options, snapshot
        _unsupported_cursor()

    async def select_search_cursor(
        self,
        return_type: type[T],
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,
        cursor: CursorPaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
    ) -> SearchCursorPage[T]:
        _ = return_type, query, filters, cursor, sorts, options
        _unsupported_cursor()

    # ....................... #
    # Streaming rides the keyset cursor, which these offset-only adapters do not serve,
    # so it fails closed too (they also advertise ``supports_stream=False``).

    async def search_stream(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        chunk_size: int = 500,
    ) -> AsyncGenerator[Sequence[M]]:
        _ = query, filters, sorts, options, chunk_size
        _unsupported_cursor()
        yield []  # pragma: no cover — unreachable; marks this an async generator

    async def project_search_stream(
        self,
        fields: Sequence[str],
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        chunk_size: int = 500,
    ) -> AsyncGenerator[Sequence[JsonDict]]:
        _ = fields, query, filters, sorts, options, chunk_size
        _unsupported_cursor()
        yield []  # pragma: no cover — unreachable; marks this an async generator

    async def select_search_stream(
        self,
        return_type: type[T],
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        chunk_size: int = 500,
    ) -> AsyncGenerator[Sequence[T]]:
        _ = return_type, query, filters, sorts, options, chunk_size
        _unsupported_cursor()
        yield []  # pragma: no cover — unreachable; marks this an async generator

    async def aggregate_search(
        self,
        aggregates: AggregatesExpression,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
    ) -> CountlessPage[JsonDict]:
        _ = aggregates, query, filters, pagination, sorts, options
        # A hub merges capped legs and a federation merges unlike rows: neither has the one
        # complete matched set an aggregate reads, so both refuse as the real ones do.
        validate_aggregates_supported(DEFAULT_SEARCH_CAPABILITIES, backend=type(self).__name__)
        raise exc.internal("unreachable")  # pragma: no cover

    async def aggregate_search_page(
        self,
        aggregates: AggregatesExpression,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
    ) -> Page[JsonDict]:
        _ = aggregates, query, filters, pagination, sorts, options
        validate_aggregates_supported(DEFAULT_SEARCH_CAPABILITIES, backend=type(self).__name__)
        raise exc.internal("unreachable")  # pragma: no cover
