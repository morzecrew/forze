from typing import Any

import attrs
from pydantic import BaseModel as BM

from forze.application.contracts.document import DocumentQueryPort
from forze.application.contracts.mapping import Mapper
from forze.application.dto import (
    AggregatedListRequestDTO,
    CursorListRequestDTO,
    CursorPaginated,
    ListRequestDTO,
    Paginated,
    RawCursorListRequestDTO,
    RawCursorPaginated,
    RawListRequestDTO,
    RawPaginated,
)
from forze.application.execution import Handler

# ----------------------- #

LRD = ListRequestDTO
RRD = RawListRequestDTO
CLD = CursorListRequestDTO
RCD = RawCursorListRequestDTO
ALD = AggregatedListRequestDTO

# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class TypedListDocuments[Out: BM](Handler[LRD, Paginated[Out]]):
    """Usecase that fetches multiple documents by filters and sorts."""

    doc: DocumentQueryPort[Out]
    """Document port for list operations."""

    mapper: Mapper[LRD, LRD] | None = attrs.field(default=None)
    """Optional mapper to transform incoming request DTO"""

    # ....................... #

    async def main(self, args: LRD) -> Paginated[Out]:
        """Fetch multiple documents by filters and sorts.

        :param args: List arguments (body, page, size).
        :returns: Paginated list of read models.
        """

        page = args.page
        size = args.size
        limit = size
        offset = (page - 1) * limit
        body = args

        if self.mapper:
            body = await self.mapper(body)

        res = await self.doc.find_page(
            filters=body.filters,
            sorts=body.sorts,
            pagination={
                "limit": limit,
                "offset": offset,
            },
        )

        return Paginated.from_page(res)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class RawListDocuments(Handler[RRD, RawPaginated]):
    """Usecase that fetches multiple documents by filters and sorts with raw results."""

    doc: DocumentQueryPort[Any]
    """Document port for list operations."""

    mapper: Mapper[RRD, RRD] | None = attrs.field(default=None)
    """Optional mapper to transform incoming request DTO"""

    # ....................... #

    async def main(self, args: RRD) -> RawPaginated:
        """Fetch multiple documents by filters and sorts with raw results.

        :param args: List arguments (body, page, size).
        :returns: Paginated list of raw results.
        """

        page = args.page
        size = args.size
        limit = size
        offset = (page - 1) * limit
        body = args

        if self.mapper:
            body = await self.mapper(body)

        res = await self.doc.project_page(
            tuple(body.return_fields),
            filters=body.filters,
            sorts=body.sorts,
            pagination={
                "limit": limit,
                "offset": offset,
            },
        )

        return RawPaginated.from_page(res)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class TypedCursorListDocuments[Out: BM](Handler[CLD, CursorPaginated[Out]]):
    """Usecase that lists documents with cursor (keyset) pagination."""

    doc: DocumentQueryPort[Out]
    """Document port for list operations."""

    mapper: Mapper[CLD, CLD] | None = attrs.field(default=None)
    """Optional mapper to transform incoming request DTO"""

    # ....................... #

    async def main(self, args: CLD) -> CursorPaginated[Out]:
        body = args

        if self.mapper:
            body = await self.mapper(body)

        res = await self.doc.find_cursor(
            filters=body.filters,
            cursor=body.to_cursor_expression(),
            sorts=body.sorts,
        )

        return CursorPaginated.from_page(res)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class RawCursorListDocuments(Handler[RCD, RawCursorPaginated]):
    """Usecase that lists documents with raw projection and cursor pagination."""

    doc: DocumentQueryPort[Any]
    """Document port for list operations."""

    mapper: Mapper[RCD, RCD] | None = attrs.field(default=None)
    """Optional mapper to transform incoming request DTO"""

    # ....................... #

    async def main(self, args: RCD) -> RawCursorPaginated:
        body = args

        if self.mapper:
            body = await self.mapper(body)

        res = await self.doc.project_cursor(
            tuple(body.return_fields),
            filters=body.filters,
            cursor=body.to_cursor_expression(),
            sorts=body.sorts,
        )

        return RawCursorPaginated.from_page(res)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class AggregatedListDocuments(Handler[ALD, RawPaginated]):
    """Usecase that fetches multiple documents by filters and sorts with aggregates."""

    doc: DocumentQueryPort[Any]
    """Document port for list operations."""

    mapper: Mapper[ALD, ALD] | None = attrs.field(default=None)
    """Optional mapper to transform incoming request DTO"""

    # ....................... #

    async def main(self, args: ALD) -> RawPaginated:
        page = args.page
        size = args.size
        limit = size
        offset = (page - 1) * limit
        body = args

        if self.mapper:
            body = await self.mapper(body)

        res = await self.doc.aggregate_page(
            body.aggregates,
            filters=body.filters,
            sorts=body.sorts,
            pagination={
                "limit": limit,
                "offset": offset,
            },
        )

        return RawPaginated.from_page(res)
