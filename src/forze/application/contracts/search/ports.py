"""Search query and command port definitions.

**Cursor search (``search_with_cursor``):** SQL and vector adapters must keyset
within the same ranked ``ORDER BY`` (score columns + tie-breakers, typically
``id``). That implies declaring cursor columns in :class:`.SearchSpec` or
Postgres search config, reusing the index heap primary key where applicable.
Federated / hub merges complicate cursors; those adapters may raise
``NotImplementedError`` until a single merged ordering is defined.
"""

from collections.abc import Sequence
from typing import Awaitable, Literal, Protocol, TypeVar, overload

from pydantic import BaseModel

from forze.base.primitives import JsonDict

from ..base import CountlessPage, CursorPage, Page
from ..query import (
    CursorPaginationExpression,
    PaginationExpression,
    QueryFilterExpression,
    QuerySortExpression,
)
from .types import SearchOptions

# ----------------------- #

T = TypeVar("T", bound=BaseModel)

# ....................... #


class SearchQueryPort[R: BaseModel](Protocol):
    @overload
    def search(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        options: SearchOptions | None = ...,
        return_type: None = ...,
        return_fields: None = ...,
        return_count: Literal[False] = False,
    ) -> Awaitable[CountlessPage[R]]:
        """Search documents and return typed read models (no count query)."""
        ...  # pragma: no cover

    @overload
    def search(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        options: SearchOptions | None = ...,
        return_type: type[T],
        return_fields: None = ...,
        return_count: Literal[False] = False,
    ) -> Awaitable[CountlessPage[T]]: ...  # pragma: no cover

    @overload
    def search(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        options: SearchOptions | None = ...,
        return_type: None = ...,
        return_fields: Sequence[str],
        return_count: Literal[False] = False,
    ) -> Awaitable[CountlessPage[JsonDict]]: ...  # pragma: no cover

    @overload
    def search(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        options: SearchOptions | None = ...,
        return_type: None = ...,
        return_fields: None = ...,
        return_count: Literal[True],
    ) -> Awaitable[Page[R]]:
        """Search documents and return typed read models and total count."""
        ...  # pragma: no cover

    @overload
    def search(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        options: SearchOptions | None = ...,
        return_type: type[T],
        return_fields: None = ...,
        return_count: Literal[True],
    ) -> Awaitable[Page[T]]: ...  # pragma: no cover

    @overload
    def search(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        options: SearchOptions | None = ...,
        return_type: None = ...,
        return_fields: Sequence[str],
        return_count: Literal[True],
    ) -> Awaitable[Page[JsonDict]]: ...  # pragma: no cover

    def search(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        return_type: type[T] | None = None,
        return_fields: Sequence[str] | None = None,
        return_count: bool = False,
    ) -> Awaitable[
        CountlessPage[R]
        | CountlessPage[T]
        | CountlessPage[JsonDict]
        | Page[R]
        | Page[T]
        | Page[JsonDict]
    ]:
        """Search documents using a query string and optional filters.

        When ``return_count`` is ``True``, returns a :class:`~.Page` with
        ``count``; otherwise a :class:`~.CountlessPage` (no total).
        """
        ...  # pragma: no cover

    # ....................... #

    @overload
    def search_with_cursor(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        options: SearchOptions | None = ...,
        return_type: None = ...,
        return_fields: None = ...,
    ) -> Awaitable[CursorPage[R]]: ...  # pragma: no cover

    @overload
    def search_with_cursor(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        options: SearchOptions | None = ...,
        return_type: type[T],
        return_fields: None = ...,
    ) -> Awaitable[CursorPage[T]]: ...  # pragma: no cover

    @overload
    def search_with_cursor(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        options: SearchOptions | None = ...,
        return_type: None = ...,
        return_fields: Sequence[str],
    ) -> Awaitable[CursorPage[JsonDict]]: ...  # pragma: no cover

    def search_with_cursor(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        return_type: type[T] | None = None,
        return_fields: Sequence[str] | None = None,
    ) -> Awaitable[
        CursorPage[R] | CursorPage[T] | CursorPage[JsonDict]
    ]: ...  # pragma: no cover


# ....................... #


#! Not implemented yet
class SearchCommandPort[M: BaseModel](Protocol): ...  # pragma: no cover
