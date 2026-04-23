from typing import Any

import attrs
from pydantic import BaseModel

from forze.application.contracts.mapping import MapperPort
from forze.application.contracts.search import SearchQueryPort
from forze.application.dto import (
    CursorPaginated,
    CursorSearchRequestDTO,
    RawCursorPaginated,
    RawCursorSearchRequestDTO,
    RawSearchRequestDTO,
    Paginated,
    RawPaginated,
    SearchRequestDTO,
)
from forze.application.dto.paginated import to_cursor_expression
from forze.application.execution import Usecase

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class TypedSearch[Out: BaseModel](Usecase[SearchRequestDTO, Paginated[Out]]):
    """Usecase that searches with typed results."""

    search: SearchQueryPort[Out]
    """Search port for search operations."""

    mapper: MapperPort[SearchRequestDTO, SearchRequestDTO] | None = attrs.field(
        default=None
    )
    """Optional mapper to transform incoming request DTO"""

    # ....................... #

    async def main(self, args: SearchRequestDTO) -> Paginated[Out]:
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
            body = await self.mapper(body, ctx=self.ctx)

        res = await self.search.search(
            query=body.query,
            filters=body.filters,
            pagination={
                "limit": limit,
                "offset": offset,
            },
            sorts=body.sorts,
            options=body.options,
            return_count=True,
        )

        return Paginated.from_page(res)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class RawSearch(Usecase[RawSearchRequestDTO, RawPaginated]):
    """Usecase that searches with raw results."""

    search: SearchQueryPort[Any]
    """Search port for search operations."""

    mapper: MapperPort[RawSearchRequestDTO, RawSearchRequestDTO] | None = attrs.field(
        default=None
    )
    """Optional mapper to transform incoming request DTO"""

    # ....................... #

    async def main(self, args: RawSearchRequestDTO) -> RawPaginated:
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
            body = await self.mapper(body, ctx=self.ctx)

        res = await self.search.search(
            query=body.query,
            filters=body.filters,
            pagination={
                "limit": limit,
                "offset": offset,
            },
            sorts=body.sorts,
            options=body.options,
            return_fields=tuple(body.return_fields),
            return_count=True,
        )

        return RawPaginated.from_page(res)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class TypedCursorSearch[Out: BaseModel](Usecase[CursorSearchRequestDTO, CursorPaginated[Out]]):
    """Usecase that searches with typed results and cursor (keyset) pagination."""

    search: SearchQueryPort[Out]
    """Search port for search operations."""

    mapper: MapperPort[CursorSearchRequestDTO, CursorSearchRequestDTO] | None = attrs.field(
        default=None
    )
    """Optional mapper to transform incoming request DTO"""

    # ....................... #

    async def main(self, args: CursorSearchRequestDTO) -> CursorPaginated[Out]:
        body = args

        if self.mapper:
            body = await self.mapper(body, ctx=self.ctx)

        res = await self.search.search_with_cursor(
            query=body.query,
            filters=body.filters,
            cursor=to_cursor_expression(body),
            sorts=body.sorts,
            options=body.options,
        )

        return CursorPaginated.from_page(res)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class RawCursorSearch(Usecase[RawCursorSearchRequestDTO, RawCursorPaginated]):
    """Usecase that searches with raw results and cursor (keyset) pagination."""

    search: SearchQueryPort[Any]
    """Search port for search operations."""

    mapper: MapperPort[RawCursorSearchRequestDTO, RawCursorSearchRequestDTO] | None = (
        attrs.field(default=None)
    )
    """Optional mapper to transform incoming request DTO"""

    # ....................... #

    async def main(self, args: RawCursorSearchRequestDTO) -> RawCursorPaginated:
        body = args

        if self.mapper:
            body = await self.mapper(body, ctx=self.ctx)

        res = await self.search.search_with_cursor(
            query=body.query,
            filters=body.filters,
            cursor=to_cursor_expression(body),
            sorts=body.sorts,
            options=body.options,
            return_fields=tuple(body.return_fields),
        )

        return RawCursorPaginated.from_page(res)
