from typing import Any, TypedDict, final

import attrs

from forze.application.dto.paginated import Paginated, RawPaginated
from forze.application.dto.search import RawSearchRequestDTO, SearchRequestDTO
from forze.application.kernel.ports import DocumentPort
from forze.application.kernel.usecase import Usecase
from forze.domain.models import ReadDocument

# ----------------------- #


@final
class SearchArgs(TypedDict):
    body: SearchRequestDTO
    page: int
    size: int


# ....................... #


@final
class RawSearchArgs(TypedDict):
    body: RawSearchRequestDTO
    page: int
    size: int


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class SearchDocument[Out: ReadDocument](Usecase[SearchArgs, Paginated[Out]]):
    doc: DocumentPort[Out, Any, Any, Any]

    # ....................... #

    async def main(self, args: SearchArgs) -> Paginated[Out]:
        body = args["body"]
        page = args["page"]
        size = args["size"]

        limit = size
        offset = (page - 1) * limit

        if body.query:
            hits, count = await self.doc.search(
                query=body.query,
                filters=body.filters,
                limit=limit,
                offset=offset,
                sorts=body.sorts,
                options={"use_fuzzy": True},
            )
        else:
            hits, count = await self.doc.find_many(
                filters=body.filters,
                limit=limit,
                offset=offset,
                sorts=body.sorts,
            )

        return Paginated(hits=hits, page=page, size=size, count=count)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class RawSearchDocument(Usecase[RawSearchArgs, RawPaginated]):
    doc: DocumentPort[Any, Any, Any, Any]

    # ....................... #

    async def main(self, args: RawSearchArgs) -> RawPaginated:
        body = args["body"]
        page = args["page"]
        size = args["size"]

        limit = size
        offset = (page - 1) * limit

        if body.query:
            hits, count = await self.doc.search(
                query=body.query,
                filters=body.filters,
                limit=limit,
                offset=offset,
                sorts=body.sorts,
                options={"use_fuzzy": True},
                return_fields=list(body.return_fields),
            )

        else:
            hits, count = await self.doc.find_many(
                filters=body.filters,
                limit=limit,
                offset=offset,
                sorts=body.sorts,
                return_fields=list(body.return_fields),
            )

        return RawPaginated(hits=hits, page=page, size=size, count=count)
