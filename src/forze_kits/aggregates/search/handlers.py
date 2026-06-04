from typing import Any

import attrs
from pydantic import BaseModel as Bm

from forze.application.contracts.execution import Handler
from forze.application.contracts.mapping import Mapper
from forze.application.contracts.search import SearchQueryPort
from forze_kits.dto import CursorPaginated, ProjectedCursorPaginated

from .dto import (
    CursorSearchRequestDTO,
    ProjectedCursorSearchRequestDTO,
    ProjectedSearchPaginated,
    ProjectedSearchRequestDTO,
    SearchPaginated,
    SearchRequestDTO,
)

# ----------------------- #

Sr = SearchRequestDTO
Psr = ProjectedSearchRequestDTO
Csr = CursorSearchRequestDTO
Pcsr = ProjectedCursorSearchRequestDTO

# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class Search[Out: Bm](Handler[Sr, SearchPaginated[Out]]):
    """Operation handler that searches with typed results."""

    search: SearchQueryPort[Out]
    """Search port for search operations."""

    mapper: Mapper[Sr, Sr] | None = attrs.field(default=None)
    """Optional mapper to transform incoming request DTO"""

    # ....................... #

    async def __call__(self, args: Sr) -> SearchPaginated[Out]:
        """Search with typed paginated results.

        :param args: Search arguments (body, page, size).
        :returns: Paginated list of read models.
        """

        body = args

        if self.mapper:
            body = await self.mapper(body)

        res = await self.search.search_page(
            query=body.query,
            filters=body.filters,
            pagination=body.to_offset_expression(),
            sorts=body.sorts,
            options=body.options,
            snapshot=body.snapshot,
        )

        return SearchPaginated.from_page(res)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class ProjectedSearch(Handler[Psr, ProjectedSearchPaginated]):
    """Operation handler that searches with field-projected raw results."""

    search: SearchQueryPort[Any]
    """Search port for search operations."""

    mapper: Mapper[Psr, Psr] | None = attrs.field(default=None)
    """Optional mapper to transform incoming request DTO"""

    # ....................... #

    async def __call__(self, args: Psr) -> ProjectedSearchPaginated:
        """Search with raw results.

        :param args: Search arguments (body, page, size).
        :returns: Paginated list of raw results.
        """

        body = args

        if self.mapper:
            body = await self.mapper(body)

        res = await self.search.project_search_page(
            tuple(body.return_fields),
            query=body.query,
            filters=body.filters,
            pagination=body.to_offset_expression(),
            sorts=body.sorts,
            options=body.options,
            snapshot=body.snapshot,
        )

        return ProjectedSearchPaginated.from_page(res)


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
class ProjectedCursorSearch(Handler[Pcsr, ProjectedCursorPaginated]):
    """Operation handler that searches with raw results and cursor (keyset) pagination."""

    search: SearchQueryPort[Any]
    """Search port for search operations."""

    mapper: Mapper[Pcsr, Pcsr] | None = attrs.field(default=None)
    """Optional mapper to transform incoming request DTO"""

    # ....................... #

    async def __call__(self, args: Pcsr) -> ProjectedCursorPaginated:
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

        return ProjectedCursorPaginated.from_page(res)
