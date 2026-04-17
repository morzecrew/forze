from typing import Any

import attrs

from forze.application.contracts.document import DocumentQueryPort
from forze.application.contracts.mapping import MapperPort
from forze.application.dto import (
    ListRequestDTO,
    Paginated,
    RawListRequestDTO,
    RawPaginated,
)
from forze.application.execution import Usecase
from forze.domain.models import ReadDocument

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class TypedListDocuments[Out: ReadDocument](Usecase[ListRequestDTO, Paginated[Out]]):
    """Usecase that fetches multiple documents by filters and sorts."""

    doc: DocumentQueryPort[Out]
    """Document port for list operations."""

    mapper: MapperPort[ListRequestDTO, ListRequestDTO] | None = attrs.field(
        default=None
    )
    """Optional mapper to transform incoming request DTO"""

    # ....................... #

    async def main(self, args: ListRequestDTO) -> Paginated[Out]:
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
            body = await self.mapper(body, ctx=self.ctx)

        hits, count = await self.doc.find_many(
            filters=body.filters,
            sorts=body.sorts,
            pagination={
                "limit": limit,
                "offset": offset,
            },
        )

        return Paginated(hits=hits, page=page, size=size, count=count)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class RawListDocuments(Usecase[RawListRequestDTO, RawPaginated]):
    """Usecase that fetches multiple documents by filters and sorts with raw results."""

    doc: DocumentQueryPort[Any]
    """Document port for list operations."""

    mapper: MapperPort[RawListRequestDTO, RawListRequestDTO] | None = attrs.field(
        default=None
    )
    """Optional mapper to transform incoming request DTO"""

    # ....................... #

    async def main(self, args: RawListRequestDTO) -> RawPaginated:
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
            body = await self.mapper(body, ctx=self.ctx)

        hits, count = await self.doc.find_many(
            filters=body.filters,
            sorts=body.sorts,
            pagination={
                "limit": limit,
                "offset": offset,
            },
            return_fields=tuple(body.return_fields),
        )

        return RawPaginated(hits=hits, page=page, size=size, count=count)
