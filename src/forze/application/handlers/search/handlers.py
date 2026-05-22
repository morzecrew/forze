from typing import Any

import attrs
from pydantic import BaseModel as Bm

from forze.application.contracts.execution import Handler
from forze.application.contracts.mapping import Mapper
from forze.application.contracts.search import SearchQueryPort
from forze.application.dto import (
    CursorPaginated,
    Paginated,
    RawCursorPaginated,
    RawPaginated,
)

from .dto import (
    CursorSearchRequestDTO,
    ProjectedCursorSearchRequestDTO,
    ProjectedSearchRequestDTO,
    SearchRequestDTO,
)

# ----------------------- #

Sr = SearchRequestDTO
Psr = ProjectedSearchRequestDTO
Csr = CursorSearchRequestDTO
Pcsr = ProjectedCursorSearchRequestDTO

# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class Search[Out: Bm](Handler[Sr, Paginated[Out]]):
    """Operation handler that searches with typed results."""

    search: SearchQueryPort[Out]
    """Search port for search operations."""

    mapper: Mapper[Sr, Sr] | None = attrs.field(default=None)
    """Optional mapper to transform incoming request DTO"""

    # ....................... #

    async def __call__(self, args: Sr) -> Paginated[Out]:
        """Search with typed paginated results.

        :param args: Search arguments (body, page, size).
        :returns: Paginated list of read models.
        """

        page = args.page
        size = args.size
        limit = size
        offset = (page - 1) * limit
        body = args

        if self.mapper:
            body = await self.mapper(body)

        res = await self.search.search_page(
            query=body.query,
            filters=body.filters,
            pagination={
                "limit": limit,
                "offset": offset,
            },
            sorts=body.sorts,
            options=body.options,
            snapshot=body.snapshot,
        )

        return Paginated.from_page(res)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class ProjectedSearch(Handler[Psr, RawPaginated]):
    """Operation handler that searches with field-projected raw results."""

    search: SearchQueryPort[Any]
    """Search port for search operations."""

    mapper: Mapper[Psr, Psr] | None = attrs.field(default=None)
    """Optional mapper to transform incoming request DTO"""

    # ....................... #

    async def __call__(self, args: Psr) -> RawPaginated:
        """Search with raw results.

        :param args: Search arguments (body, page, size).
        :returns: Paginated list of raw results.
        """

        page = args.page
        size = args.size
        limit = size
        offset = (page - 1) * limit
        body = args

        if self.mapper:
            body = await self.mapper(body)

        res = await self.search.project_search_page(
            tuple(body.return_fields),
            query=body.query,
            filters=body.filters,
            pagination={
                "limit": limit,
                "offset": offset,
            },
            sorts=body.sorts,
            options=body.options,
            snapshot=body.snapshot,
        )

        return RawPaginated.from_page(res)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class CursorSearch[Out: Bm](Handler[Csr, CursorPaginated[Out]]):
    """Operation handler that searches with typed results and cursor (keyset) pagination."""

    search: SearchQueryPort[Out]
    """Search port for search operations."""

    mapper: Mapper[Csr, Csr] | None = attrs.field(default=None)
    """Optional mapper to transform incoming request DTO"""

    # ....................... #

    async def __call__(self, args: Csr) -> CursorPaginated[Out]:
        body = args

        if self.mapper:
            body = await self.mapper(body)

        res = await self.search.search_cursor(
            query=body.query,
            filters=body.filters,
            cursor=body.to_cursor_expression(),
            sorts=body.sorts,
            options=body.options,
        )

        return CursorPaginated.from_page(res)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class ProjectedCursorSearch(Handler[Pcsr, RawCursorPaginated]):
    """Operation handler that searches with raw results and cursor (keyset) pagination."""

    search: SearchQueryPort[Any]
    """Search port for search operations."""

    mapper: Mapper[Pcsr, Pcsr] | None = attrs.field(default=None)
    """Optional mapper to transform incoming request DTO"""

    # ....................... #

    async def __call__(self, args: Pcsr) -> RawCursorPaginated:
        body = args

        if self.mapper:
            body = await self.mapper(body)

        res = await self.search.project_search_cursor(
            tuple(body.return_fields),
            query=body.query,
            filters=body.filters,
            cursor=body.to_cursor_expression(),
            sorts=body.sorts,
            options=body.options,
        )

        return RawCursorPaginated.from_page(res)
