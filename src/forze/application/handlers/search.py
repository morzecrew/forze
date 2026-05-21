from typing import Any

import attrs
from pydantic import BaseModel as BM

from forze.application.contracts.mapping import Mapper
from forze.application.contracts.search import SearchQueryPort
from forze.application.dto import (
    CursorPaginated,
    CursorSearchRequestDTO,
    Paginated,
    RawCursorPaginated,
    RawCursorSearchRequestDTO,
    RawPaginated,
    RawSearchRequestDTO,
    SearchRequestDTO,
)
from forze.application.execution.core import Handler

# ----------------------- #

SRD = SearchRequestDTO
RRD = RawSearchRequestDTO
CSR = CursorSearchRequestDTO
RCD = RawCursorSearchRequestDTO

# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class TypedSearch[Out: BM](Handler[SRD, Paginated[Out]]):
    """Usecase that searches with typed results."""

    search: SearchQueryPort[Out]
    """Search port for search operations."""

    mapper: Mapper[SRD, SRD] | None = attrs.field(default=None)
    """Optional mapper to transform incoming request DTO"""

    # ....................... #

    async def __call__(self, args: SRD) -> Paginated[Out]:
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
class RawSearch(Handler[RRD, RawPaginated]):
    """Usecase that searches with raw results."""

    search: SearchQueryPort[Any]
    """Search port for search operations."""

    mapper: Mapper[RRD, RRD] | None = attrs.field(default=None)
    """Optional mapper to transform incoming request DTO"""

    # ....................... #

    async def __call__(self, args: RRD) -> RawPaginated:
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
class TypedCursorSearch[Out: BM](Handler[CSR, CursorPaginated[Out]]):
    """Usecase that searches with typed results and cursor (keyset) pagination."""

    search: SearchQueryPort[Out]
    """Search port for search operations."""

    mapper: Mapper[CSR, CSR] | None = attrs.field(default=None)
    """Optional mapper to transform incoming request DTO"""

    # ....................... #

    async def __call__(self, args: CSR) -> CursorPaginated[Out]:
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
class RawCursorSearch(Handler[RCD, RawCursorPaginated]):
    """Usecase that searches with raw results and cursor (keyset) pagination."""

    search: SearchQueryPort[Any]
    """Search port for search operations."""

    mapper: Mapper[RCD, RCD] | None = attrs.field(default=None)
    """Optional mapper to transform incoming request DTO"""

    # ....................... #

    async def __call__(self, args: RCD) -> RawCursorPaginated:
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
