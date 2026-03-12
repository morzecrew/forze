from typing import Any, Optional, TypedDict

import attrs

from forze.application.contracts.document import DocumentReadPort
from forze.application.dto import (
    ListRequestDTO,
    Paginated,
    RawListRequestDTO,
    RawPaginated,
)
from forze.application.execution import Usecase
from forze.application.mapping import DTOMapper
from forze.domain.models import ReadDocument

# ----------------------- #


class TypedListDocumentsArgs[In: ListRequestDTO](TypedDict):
    """Arguments for typed list documents usecase."""

    body: In
    """List request (filters, sorts)."""

    page: int
    """One-based page number."""

    size: int
    """Page size."""


# ....................... #


class RawListDocumentsArgs[In: RawListRequestDTO](TypedDict):
    """Arguments for raw (field-projected) list documents usecase."""

    body: In
    """List request with required ``return_fields``."""

    page: int
    """One-based page number."""

    size: int
    """Page size."""


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class TypedListDocuments[In: ListRequestDTO, Out: ReadDocument](
    Usecase[TypedListDocumentsArgs[In], Paginated[Out]]
):
    """Usecase that fetches multiple documents by filters and sorts."""

    doc: DocumentReadPort[Out]
    """Read-only document port for list operations."""

    mapper: Optional[DTOMapper[In, ListRequestDTO]] = None
    """Optional mapper to transform incoming request DTO"""

    # ....................... #

    async def main(self, args: TypedListDocumentsArgs[In]) -> Paginated[Out]:
        """Fetch multiple documents by filters and sorts.

        :param args: List arguments (body, page, size).
        :returns: Paginated list of read models.
        """

        body = args["body"]
        page = args["page"]
        size = args["size"]

        limit = size
        offset = (page - 1) * limit

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
class RawListDocuments[In: RawListRequestDTO](
    Usecase[RawListDocumentsArgs[In], RawPaginated]
):
    """Usecase that fetches multiple documents by filters and sorts with raw results."""

    doc: DocumentReadPort[Any]
    """Read-only document port for list operations."""

    mapper: Optional[DTOMapper[In, RawListRequestDTO]] = None
    """Optional mapper to transform incoming request DTO"""

    # ....................... #

    async def main(self, args: RawListDocumentsArgs[In]) -> RawPaginated:
        """Fetch multiple documents by filters and sorts with raw results.

        :param args: List arguments (body, page, size).
        :returns: Paginated list of raw results.
        """

        body = args["body"]
        page = args["page"]
        size = args["size"]

        limit = size
        offset = (page - 1) * limit

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
