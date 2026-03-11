from typing import Any, Optional, TypedDict, final

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


@final
class TypedListDocumentsArgs(TypedDict):
    """Arguments for typed list documents usecase."""

    body: ListRequestDTO
    """List request (filters, sorts)."""

    page: int
    """One-based page number."""

    size: int
    """Page size."""


# ....................... #


@final
class RawListDocumentsArgs(TypedDict):
    """Arguments for raw (field-projected) list documents usecase."""

    body: RawListRequestDTO
    """List request with required ``return_fields``."""

    page: int
    """One-based page number."""

    size: int
    """Page size."""


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class TypedListDocuments[Out: ReadDocument](
    Usecase[TypedListDocumentsArgs, Paginated[Out]]
):
    """Usecase that fetches multiple documents by filters and sorts."""

    doc: DocumentReadPort[Out]
    """Read-only document port for list operations."""

    mapper: Optional[DTOMapper[ListRequestDTO]] = None
    """Optional mapper to transform incoming request DTO"""

    # ....................... #

    async def main(self, args: TypedListDocumentsArgs) -> Paginated[Out]:
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
            body = await self.mapper(self.ctx, body)

        hits, count = await self.doc.find_many(
            filters=body.filters,
            sorts=body.sorts,
            limit=limit,
            offset=offset,
        )

        return Paginated(hits=hits, page=page, size=size, count=count)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class RawListDocuments(Usecase[RawListDocumentsArgs, RawPaginated]):
    """Usecase that fetches multiple documents by filters and sorts with raw results."""

    doc: DocumentReadPort[Any]
    """Read-only document port for list operations."""

    mapper: Optional[DTOMapper[RawListRequestDTO]] = None
    """Optional mapper to transform incoming request DTO"""

    # ....................... #

    async def main(self, args: RawListDocumentsArgs) -> RawPaginated:
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
            body = await self.mapper(self.ctx, body)

        hits, count = await self.doc.find_many(
            filters=body.filters,
            sorts=body.sorts,
            limit=limit,
            offset=offset,
        )

        return RawPaginated(hits=hits, page=page, size=size, count=count)
