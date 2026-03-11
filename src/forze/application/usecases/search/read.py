from typing import Any, Optional, TypedDict, final

import attrs
from pydantic import BaseModel

from forze.application.contracts.search import SearchReadPort
from forze.application.dto import (
    Paginated,
    RawPaginated,
    RawSearchRequestDTO,
    SearchRequestDTO,
)
from forze.application.execution import Usecase
from forze.application.mapping import DTOMapper

# ----------------------- #


@final
class TypedSearchArgs(TypedDict):
    """Arguments for typed search usecases."""

    body: SearchRequestDTO
    """Search request (query, filters, sorts)."""

    page: int
    """One-based page number."""

    size: int
    """Page size."""


# ....................... #


@final
class RawSearchArgs(TypedDict):
    """Arguments for raw (field-projected) search usecases."""

    body: RawSearchRequestDTO
    """Search request with required ``return_fields``."""

    page: int
    """One-based page number."""

    size: int
    """Page size."""


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class TypedSearch[Out: BaseModel](Usecase[TypedSearchArgs, Paginated[Out]]):
    """Usecase that searches with typed results."""

    search: SearchReadPort[Out]
    """Search port for search operations."""

    mapper: Optional[DTOMapper[SearchRequestDTO]] = None
    """Optional mapper to transform incoming request DTO"""

    # ....................... #

    async def main(self, args: TypedSearchArgs) -> Paginated[Out]:
        """Search with typed paginated results.

        :param args: Search arguments (body, page, size).
        :returns: Paginated list of read models.
        """
        body = args["body"]
        page = args["page"]
        size = args["size"]

        limit = size
        offset = (page - 1) * limit

        if self.mapper:
            body = await self.mapper(self.ctx, body)

        hits, count = await self.search.search(
            query=body.query,
            filters=body.filters,
            limit=limit,
            offset=offset,
            sorts=body.sorts,
            options=body.options,
        )

        return Paginated(hits=hits, page=page, size=size, count=count)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class RawSearch(Usecase[RawSearchArgs, RawPaginated]):
    """Usecase that searches with raw results."""

    search: SearchReadPort[Any]
    """Search port for search operations."""

    mapper: Optional[DTOMapper[RawSearchRequestDTO]] = None
    """Optional mapper to transform incoming request DTO"""

    # ....................... #

    async def main(self, args: RawSearchArgs) -> RawPaginated:
        """Search with raw results.

        :param args: Search arguments (body, page, size).
        :returns: Paginated list of raw results."""

        body = args["body"]
        page = args["page"]
        size = args["size"]

        limit = size
        offset = (page - 1) * limit

        if self.mapper:
            body = await self.mapper(self.ctx, body)

        hits, count = await self.search.search(
            query=body.query,
            filters=body.filters,
            limit=limit,
            offset=offset,
            sorts=body.sorts,
            options=body.options,
            return_fields=list(body.return_fields),
        )

        return RawPaginated(hits=hits, page=page, size=size, count=count)
