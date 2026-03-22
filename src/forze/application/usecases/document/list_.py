from typing import Any

import attrs

from forze.application.contracts.document import DocumentReadPort
from forze.application.contracts.mapper import MapperPort
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

    doc: DocumentReadPort[Out]
    """Read-only document port for list operations."""

    mapper: MapperPort[ListRequestDTO, ListRequestDTO] | None = None
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
            # typevar ensures that the incoming body is subclass of ListRequestDTO, so the assignment is safe
            body = await self.mapper(self.ctx, body)  # type: ignore[assignment]

        hits, count = await self.doc.find_many(
            filters=body.filters,
            sorts=body.sorts,
            limit=limit,
            offset=offset,
        )

        return Paginated(hits=hits, page=page, size=size, count=count)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class RawListDocuments(Usecase[RawListRequestDTO, RawPaginated]):
    """Usecase that fetches multiple documents by filters and sorts with raw results."""

    doc: DocumentReadPort[Any]
    """Read-only document port for list operations."""

    mapper: MapperPort[RawListRequestDTO, RawListRequestDTO] | None = None
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
            # typevar ensures that the incoming body is subclass of RawListRequestDTO, so the assignment is safe
            body = await self.mapper(self.ctx, body)  # type: ignore[assignment]

        hits, count = await self.doc.find_many(
            filters=body.filters,
            sorts=body.sorts,
            limit=limit,
            offset=offset,
        )

        return RawPaginated(hits=hits, page=page, size=size, count=count)
