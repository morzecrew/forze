from typing import Awaitable, Protocol, Sequence, TypeVar, overload

from pydantic import BaseModel

from forze.base.primitives import JsonDict

from ..query import PaginationExpression, QueryFilterExpression, QuerySortExpression
from .types import SearchOptions

# ----------------------- #

T = TypeVar("T", bound=BaseModel)

# ....................... #
#! Consider wrapping many arguments into a single model / value object


class SearchQueryPort[R: BaseModel](Protocol):
    @overload
    def search(
        self,
        query: str,
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        options: SearchOptions | None = ...,
        return_type: None = ...,
        return_fields: None = ...,
    ) -> Awaitable[tuple[list[R], int]]:
        """Search documents and return typed read models."""

        ...  # pragma: no cover

    @overload
    def search(
        self,
        query: str,
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        options: SearchOptions | None = ...,
        return_type: type[T],
        return_fields: None = ...,
    ) -> Awaitable[tuple[list[T], int]]: ...  # pragma: no cover

    @overload
    def search(
        self,
        query: str,
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        options: SearchOptions | None = ...,
        return_type: None = ...,
        return_fields: Sequence[str],
    ) -> Awaitable[tuple[list[JsonDict], int]]:
        """Search documents and project selected fields as JSON."""

        ...  # pragma: no cover

    def search(
        self,
        query: str,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        return_type: type[T] | None = None,
        return_fields: Sequence[str] | None = None,
    ) -> Awaitable[tuple[list[R] | list[T] | list[JsonDict], int]]:
        """Search documents using a query string and optional filters.

        :param query: Query expression interpreted by the backend.
        :param filters: Structured filters applied before scoring.
        :param pagination: Pagination expression.
        :param sorts: Field-level sort specification.
        :param options: Backend-specific tuning options.
        :param return_type: Optional model-based projection to return.
        :param return_fields: Optional projection of fields.
        :returns: A tuple of hits and total hit count.
        """
        ...  # pragma: no cover


# ....................... #


#! Not implemented yet
class SearchCommandPort[M: BaseModel](Protocol): ...  # pragma: no cover
