from typing import Awaitable, Optional, Protocol, Sequence, overload

from pydantic import BaseModel

from forze.base.primitives import JsonDict

from ..query import QueryFilterExpression, QuerySortExpression
from .types import SearchOptions

# ----------------------- #


class SearchReadPort[R: BaseModel](Protocol):
    @overload
    def search(
        self,
        query: str,
        filters: Optional[QueryFilterExpression] = ...,
        limit: Optional[int] = ...,
        offset: Optional[int] = ...,
        sorts: Optional[QuerySortExpression] = ...,
        *,
        options: Optional[SearchOptions] = ...,
        return_fields: Sequence[str],
    ) -> Awaitable[tuple[list[JsonDict], int]]:
        """Search documents and project selected fields as JSON."""

        ...

    @overload
    def search(
        self,
        query: str,
        filters: Optional[QueryFilterExpression] = ...,
        limit: Optional[int] = ...,
        offset: Optional[int] = ...,
        sorts: Optional[QuerySortExpression] = ...,
        *,
        options: Optional[SearchOptions] = ...,
        return_fields: None = ...,
    ) -> Awaitable[tuple[list[R], int]]:
        """Search documents and return typed read models."""

        ...

    def search(
        self,
        query: str,
        filters: Optional[QueryFilterExpression] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        sorts: Optional[QuerySortExpression] = None,
        options: Optional[SearchOptions] = None,
        *,
        return_fields: Optional[Sequence[str]] = None,
    ) -> Awaitable[tuple[list[R] | list[JsonDict], int]]:
        """Search documents using a query string and optional filters.

        :param query: Query expression interpreted by the backend.
        :param filters: Structured filters applied before scoring.
        :param limit: Maximum number of hits to return.
        :param offset: Offset into the result set.
        :param sorts: Field-level sort specification.
        :param options: Backend-specific tuning options.
        :param return_fields: Optional projection of fields.
        :returns: A tuple of hits and total hit count.
        """
        ...


# ....................... #


class SearchWritePort[M: BaseModel](Protocol): ...
