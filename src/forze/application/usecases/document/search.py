from typing import Any, TypedDict, final

import attrs

from forze.application.contracts.document import DocumentPort
from forze.application.dto import (
    Paginated,
    RawPaginated,
    RawSearchRequestDTO,
    SearchRequestDTO,
)
from forze.application.execution import Usecase
from forze.domain.models import ReadDocument

# ----------------------- #


@final
class SearchArgs(TypedDict):
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
class SearchDocument[Out: ReadDocument](Usecase[SearchArgs, Paginated[Out]]):
    """Usecase that searches documents with typed results.

    Dispatches to :meth:`DocumentPort.search` when a query is present, otherwise
    to :meth:`DocumentPort.find_many`. Returns a :class:`Paginated` of read
    models. Uses fuzzy matching when query is provided.
    """

    doc: DocumentPort[Out, Any, Any, Any]
    """Document port for search operations."""

    # ....................... #

    async def main(self, args: SearchArgs) -> Paginated[Out]:
        """Search documents with typed paginated results.

        :param args: Search arguments (body, page, size).
        :returns: Paginated list of read models.
        """
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
    """Usecase that searches documents with field-projected raw results.

    Same as :class:`SearchDocument` but returns :class:`RawPaginated` with
    raw dict hits instead of typed models. Requires ``return_fields`` in the
    request body.
    """

    doc: DocumentPort[Any, Any, Any, Any]
    """Document port for search operations."""

    # ....................... #

    async def main(self, args: RawSearchArgs) -> RawPaginated:
        """Search documents with field-projected raw results.

        :param args: Raw search arguments (body with return_fields, page, size).
        :returns: Paginated list of JSON dicts.
        """
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
