from typing import Any, Optional, TypedDict

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


class TypedSearchArgs[In: SearchRequestDTO](TypedDict):
    """Arguments for typed search usecases."""

    body: In
    """Search request (query, filters, sorts)."""

    page: int
    """One-based page number."""

    size: int
    """Page size."""


# ....................... #


class RawSearchArgs[In: RawSearchRequestDTO](TypedDict):
    """Arguments for raw (field-projected) search usecases."""

    body: In
    """Search request with required ``return_fields``."""

    page: int
    """One-based page number."""

    size: int
    """Page size."""


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class TypedSearch[In: SearchRequestDTO, Out: BaseModel](
    Usecase[TypedSearchArgs[In], Paginated[Out]]
):
    """Usecase that searches with typed results."""

    search: SearchReadPort[Out]
    """Search port for search operations."""

    mapper: Optional[DTOMapper[In, SearchRequestDTO]] = None
    """Optional mapper to transform incoming request DTO"""

    # ....................... #

    async def main(self, args: TypedSearchArgs[In]) -> Paginated[Out]:
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
            # typevar ensures that the incoming body is subclass of SearchRequestDTO, so the assignment is safe
            body = await self.mapper(self.ctx, body)  # type: ignore[assignment]

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
class RawSearch[In: RawSearchRequestDTO](Usecase[RawSearchArgs[In], RawPaginated]):
    """Usecase that searches with raw results."""

    search: SearchReadPort[Any]
    """Search port for search operations."""

    mapper: Optional[DTOMapper[In, RawSearchRequestDTO]] = None
    """Optional mapper to transform incoming request DTO"""

    # ....................... #

    async def main(self, args: RawSearchArgs[In]) -> RawPaginated:
        """Search with raw results.

        :param args: Search arguments (body, page, size).
        :returns: Paginated list of raw results.
        """

        body = args["body"]
        page = args["page"]
        size = args["size"]

        limit = size
        offset = (page - 1) * limit

        if self.mapper:
            # typevar ensures that the incoming body is subclass of RawSearchRequestDTO, so the assignment is safe
            body = await self.mapper(self.ctx, body)  # type: ignore[assignment]

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
