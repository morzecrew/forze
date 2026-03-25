from typing import Awaitable, Protocol, Sequence, TypeVar, overload

from pydantic import BaseModel

from forze.base.primitives import JsonDict

from ..query import QueryFilterExpression, QuerySortExpression
from .types import SearchOptions

# ----------------------- #

T = TypeVar("T", bound=BaseModel)

# ....................... #


class SearchReadPort[R: BaseModel](Protocol):
    @overload
    def search(
        self,
        query: str,
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        limit: int | None = ...,
        offset: int | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        options: SearchOptions | None = ...,
        return_type: None = ...,
        return_fields: None = ...,
    ) -> Awaitable[tuple[list[R], int]]:
        """Search documents and return typed read models."""

        ...

    @overload
    def search(
        self,
        query: str,
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        limit: int | None = ...,
        offset: int | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        options: SearchOptions | None = ...,
        return_type: type[T],
        return_fields: None = ...,
    ) -> Awaitable[tuple[list[T], int]]: ...

    @overload
    def search(
        self,
        query: str,
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        limit: int | None = ...,
        offset: int | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        options: SearchOptions | None = ...,
        return_type: None = ...,
        return_fields: Sequence[str],
    ) -> Awaitable[tuple[list[JsonDict], int]]:
        """Search documents and project selected fields as JSON."""

        ...

    def search(
        self,
        query: str,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        limit: int | None = None,
        offset: int | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        return_type: type[T] | None = None,
        return_fields: Sequence[str] | None = None,
    ) -> Awaitable[tuple[list[R] | list[T] | list[JsonDict], int]]:
        """Search documents using a query string and optional filters.

        :param query: Query expression interpreted by the backend.
        :param filters: Structured filters applied before scoring.
        :param limit: Maximum number of hits to return.
        :param offset: Offset into the result set.
        :param sorts: Field-level sort specification.
        :param options: Backend-specific tuning options.
        :param return_type: Optional model-based projection to return.
        :param return_fields: Optional projection of fields.
        :returns: A tuple of hits and total hit count.
        """
        ...


# ....................... #


#! Not implemented yet
class SearchWritePort[M: BaseModel](Protocol): ...
