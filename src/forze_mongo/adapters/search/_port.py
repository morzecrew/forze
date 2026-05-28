"""Shared :class:`~forze.application.contracts.search.SearchQueryPort` method implementations."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Literal, TypeVar, overload

from pydantic import BaseModel

from forze.application.contracts.base import CountlessPage, CursorPage, Page
from forze.application.contracts.querying import (
    CursorPaginationExpression,
    PaginationExpression,
    QueryFilterExpression,
    QuerySortExpression,
)
from forze.application.contracts.search import (
    SearchOptions,
    SearchResultSnapshotOptions,
)

# ----------------------- #

T = TypeVar("T", bound=BaseModel)
M = TypeVar("M", bound=BaseModel)


class MongoSearchPortMixin[M: BaseModel]:
    """Mixin delegating search port methods to ``_offset_search_impl`` / ``_cursor_search_impl``."""

    spec: Any
    model_type: type[M]

    # ....................... #

    @overload
    async def _offset_search_impl(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        snapshot: SearchResultSnapshotOptions | None = None,
        return_count: Literal[False],
        return_type: None = None,
        return_fields: None = None,
    ) -> CountlessPage[M]: ...

    @overload
    async def _offset_search_impl(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        snapshot: SearchResultSnapshotOptions | None = None,
        return_count: Literal[True],
        return_type: None = None,
        return_fields: None = None,
    ) -> Page[M]: ...

    @overload
    async def _offset_search_impl(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        snapshot: SearchResultSnapshotOptions | None = None,
        return_count: Literal[False],
        return_type: type[T],
        return_fields: None = None,
    ) -> CountlessPage[T]: ...

    @overload
    async def _offset_search_impl(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        snapshot: SearchResultSnapshotOptions | None = None,
        return_count: Literal[True],
        return_type: type[T],
        return_fields: None = None,
    ) -> Page[T]: ...

    @overload
    async def _offset_search_impl(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        snapshot: SearchResultSnapshotOptions | None = None,
        return_count: Literal[False],
        return_type: None = None,
        return_fields: Sequence[str],
    ) -> CountlessPage[Any]: ...

    @overload
    async def _offset_search_impl(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        snapshot: SearchResultSnapshotOptions | None = None,
        return_count: Literal[True],
        return_type: None = None,
        return_fields: Sequence[str],
    ) -> Page[Any]: ...

    async def _offset_search_impl(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        snapshot: SearchResultSnapshotOptions | None = None,
        return_count: bool,
        return_type: type[BaseModel] | None = None,
        return_fields: Sequence[str] | None = None,
    ) -> Any:
        raise NotImplementedError

    async def _cursor_search_impl(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,
        cursor: CursorPaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        return_type: type[BaseModel] | None = None,
        return_fields: Sequence[str] | None = None,
    ) -> Any:
        raise NotImplementedError

    # ....................... #

    async def search(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        snapshot: SearchResultSnapshotOptions | None = None,
    ) -> CountlessPage[M]:
        return await self._offset_search_impl(
            query,
            filters,
            pagination,
            sorts,
            options=options,
            snapshot=snapshot,
            return_count=False,
        )

    async def search_page(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        snapshot: SearchResultSnapshotOptions | None = None,
    ) -> Page[M]:
        return await self._offset_search_impl(
            query,
            filters,
            pagination,
            sorts,
            options=options,
            snapshot=snapshot,
            return_count=True,
        )

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
    ) -> CountlessPage[Any]:
        return await self._offset_search_impl(
            query,
            filters,
            pagination,
            sorts,
            options=options,
            snapshot=snapshot,
            return_count=False,
            return_fields=tuple(fields),
        )

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
    ) -> Page[Any]:
        return await self._offset_search_impl(
            query,
            filters,
            pagination,
            sorts,
            options=options,
            snapshot=snapshot,
            return_count=True,
            return_fields=tuple(fields),
        )

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
    ) -> CountlessPage[T]:
        return await self._offset_search_impl(
            query,
            filters,
            pagination,
            sorts,
            options=options,
            snapshot=snapshot,
            return_count=False,
            return_type=return_type,
        )

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
    ) -> Page[T]:
        return await self._offset_search_impl(
            query,
            filters,
            pagination,
            sorts,
            options=options,
            snapshot=snapshot,
            return_count=True,
            return_type=return_type,
        )

    async def search_cursor(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,
        cursor: CursorPaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
    ) -> CursorPage[M]:
        return await self._cursor_search_impl(
            query,
            filters,
            cursor,
            sorts,
            options=options,
        )

    async def project_search_cursor(
        self,
        fields: Sequence[str],
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,
        cursor: CursorPaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
    ) -> CursorPage[Any]:
        return await self._cursor_search_impl(
            query,
            filters,
            cursor,
            sorts,
            options=options,
            return_fields=tuple(fields),
        )

    async def select_search_cursor(
        self,
        return_type: type[T],
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,
        cursor: CursorPaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
    ) -> CursorPage[T]:
        return await self._cursor_search_impl(
            query,
            filters,
            cursor,
            sorts,
            options=options,
            return_type=return_type,
        )
