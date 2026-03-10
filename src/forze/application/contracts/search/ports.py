from typing import Awaitable, Optional, Protocol, Sequence, TypeVar, overload

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
        filters: Optional[QueryFilterExpression] = ...,  # type: ignore[valid-type]
        limit: Optional[int] = ...,
        offset: Optional[int] = ...,
        sorts: Optional[QuerySortExpression] = ...,
        *,
        options: Optional[SearchOptions] = ...,
        return_model: None = ...,
        return_fields: None = ...,
    ) -> Awaitable[tuple[list[R], int]]:
        """Search documents and return typed read models."""

        ...

    @overload
    def search(
        self,
        query: str,
        filters: Optional[QueryFilterExpression] = ...,  # type: ignore[valid-type]
        limit: Optional[int] = ...,
        offset: Optional[int] = ...,
        sorts: Optional[QuerySortExpression] = ...,
        *,
        options: Optional[SearchOptions] = ...,
        return_model: type[T],
        return_fields: None = ...,
    ) -> Awaitable[tuple[list[T], int]]: ...

    @overload
    def search(
        self,
        query: str,
        filters: Optional[QueryFilterExpression] = ...,  # type: ignore[valid-type]
        limit: Optional[int] = ...,
        offset: Optional[int] = ...,
        sorts: Optional[QuerySortExpression] = ...,
        *,
        options: Optional[SearchOptions] = ...,
        return_model: None = ...,
        return_fields: Sequence[str],
    ) -> Awaitable[tuple[list[JsonDict], int]]:
        """Search documents and project selected fields as JSON."""

        ...

    def search(
        self,
        query: str,
        filters: Optional[QueryFilterExpression] = None,  # type: ignore[valid-type]
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        sorts: Optional[QuerySortExpression] = None,
        *,
        options: Optional[SearchOptions] = None,
        return_model: Optional[type[T]] = None,
        return_fields: Optional[Sequence[str]] = None,
    ) -> Awaitable[tuple[list[R] | list[T] | list[JsonDict], int]]:
        """Search documents using a query string and optional filters.

        :param query: Query expression interpreted by the backend.
        :param filters: Structured filters applied before scoring.
        :param limit: Maximum number of hits to return.
        :param offset: Offset into the result set.
        :param sorts: Field-level sort specification.
        :param options: Backend-specific tuning options.
        :param return_model: Optional model-based projection to return.
        :param return_fields: Optional projection of fields.
        :returns: A tuple of hits and total hit count.
        """
        ...


# ....................... #


class SearchWritePort[M: BaseModel](Protocol): ...
