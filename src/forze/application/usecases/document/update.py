from typing import Any, TypedDict, final
from uuid import UUID

import attrs

from forze.application.contracts.document import DocumentWritePort
from forze.application.execution import Usecase
from forze.application.mapping import DTOMapper
from forze.base.logging import getLogger, log_section
from forze.domain.models import BaseDTO, ReadDocument

# ----------------------- #

logger = getLogger(__name__)

# ....................... #


@final  #! TODO: replace with BaseDTO
class UpdateArgs[In: BaseDTO](TypedDict):
    """Arguments for update usecases."""

    pk: UUID
    """Document primary key."""

    dto: In
    """Update payload DTO."""

    rev: int
    """Expected revision for optimistic concurrency."""


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class UpdateDocument[In: BaseDTO, Cmd: BaseDTO, Out: ReadDocument](
    Usecase[UpdateArgs[In], Out]
):
    """Usecase that updates an existing document from a mapped command."""

    doc: DocumentWritePort[Out, Any, Any, Cmd]
    """Document port for update operations."""

    mapper: DTOMapper[In, Cmd]
    """Mapper that converts input DTO to update command."""

    # ....................... #

    async def main(self, args: UpdateArgs[In]) -> Out:
        """Update a document from the mapped command.

        :param args: Update arguments (pk, dto, rev).
        :returns: Updated read model.
        """

        logger.debug(
            "%s: pk=%s, rev=%s",
            type(self).__qualname__,
            args["pk"],
            args.get("rev"),
        )

        logger.debug(
            "%s: mapping input %s",
            type(self).__qualname__,
            type(args).__qualname__,
        )

        with log_section():
            cmd = await self.mapper(self.ctx, args["dto"])

        logger.debug(
            "%s: delegating to %s",
            type(self).__qualname__,
            type(self.doc).__qualname__,
        )

        with log_section():
            return await self.doc.update(args["pk"], cmd, rev=args.get("rev"))
