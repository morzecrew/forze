from typing import Any

import attrs

from forze.application.contracts.document import DocumentWritePort
from forze.application.execution import Usecase
from forze.application.mapping import DTOMapper
from forze.base.logging import getLogger, log_section
from forze.domain.models import BaseDTO, CreateDocumentCmd, ReadDocument

# ----------------------- #

logger = getLogger(__name__)

# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class CreateDocument[In: BaseDTO, Cmd: CreateDocumentCmd, Out: ReadDocument](
    Usecase[In, Out]
):
    """Usecase that creates a new document from a mapped command.

    Maps the input DTO to a :class:`CreateDocumentCmd` via :attr:`mapper`, then
    delegates to :meth:`DocumentWritePort.create`. The mapper may inject fields such
    as ``number_id`` or ``creator_id`` using execution context.
    """

    doc: DocumentWritePort[Out, Any, Cmd, Any]
    """Document port for create operations."""

    mapper: DTOMapper[In, Cmd]
    """Mapper that converts input DTO to create command."""

    # ....................... #

    async def main(self, args: In) -> Out:
        """Create a document from the mapped command.

        :param args: Input DTO (e.g. request payload).
        :returns: Created read model.
        """

        logger.debug(
            "%s: mapping input %s",
            type(self).__qualname__,
            type(args).__qualname__,
        )

        with log_section():
            cmd = await self.mapper(self.ctx, args)

        logger.debug(
            "%s: delegating to %s",
            type(self).__qualname__,
            type(self.doc).__qualname__,
        )

        with log_section():
            return await self.doc.create(cmd)
