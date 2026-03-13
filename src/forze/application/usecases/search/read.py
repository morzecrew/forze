from typing import Any, Optional

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
from forze.base.logging import getLogger, log_section

# ----------------------- #

logger = getLogger(__name__)


@attrs.define(slots=True, kw_only=True, frozen=True)
class TypedSearch[In: SearchRequestDTO, Out: BaseModel](Usecase[In, Paginated[Out]]):
    """Usecase that searches with typed results."""

    search: SearchReadPort[Out]
    """Search port for search operations."""

    mapper: Optional[DTOMapper[In, SearchRequestDTO]] = None
    """Optional mapper to transform incoming request DTO"""

    # ....................... #

    async def main(self, args: In) -> Paginated[Out]:
        """Search with typed paginated results.

        :param args: Search arguments (body, page, size).
        :returns: Paginated list of read models.
        """
        page = args.page
        size = args.size
        limit = size
        offset = (page - 1) * limit

        logger.debug(
            "%s: page=%s, size=%s, offset=%s",
            type(self).__qualname__,
            page,
            size,
            offset,
        )

        body = args

        if self.mapper:
            logger.debug(
                "%s: mapping input %s",
                type(self).__qualname__,
                type(args).__qualname__,
            )

            with log_section():
                # typevar ensures that the incoming body is subclass of SearchRequestDTO, so the assignment is safe
                body = await self.mapper(self.ctx, body)  # type: ignore[assignment]

        logger.debug(
            "%s: delegating to %s",
            type(self).__qualname__,
            type(self.search).__qualname__,
        )

        with log_section():
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
class RawSearch[In: RawSearchRequestDTO](Usecase[In, RawPaginated]):
    """Usecase that searches with raw results."""

    search: SearchReadPort[Any]
    """Search port for search operations."""

    mapper: Optional[DTOMapper[In, RawSearchRequestDTO]] = None
    """Optional mapper to transform incoming request DTO"""

    # ....................... #

    async def main(self, args: In) -> RawPaginated:
        """Search with raw results.

        :param args: Search arguments (body, page, size).
        :returns: Paginated list of raw results.
        """
        page = args.page
        size = args.size
        limit = size
        offset = (page - 1) * limit

        logger.debug(
            "%s: page=%s, size=%s, offset=%s",
            type(self).__qualname__,
            page,
            size,
            offset,
        )

        body = args

        if self.mapper:
            logger.debug(
                "%s: mapping input %s",
                type(self).__qualname__,
                type(args).__qualname__,
            )

            with log_section():
                # typevar ensures that the incoming body is subclass of RawSearchRequestDTO, so the assignment is safe
                body = await self.mapper(self.ctx, body)  # type: ignore[assignment]

        logger.debug(
            "%s: delegating to %s",
            type(self).__qualname__,
            type(self.search).__qualname__,
        )

        with log_section():
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
