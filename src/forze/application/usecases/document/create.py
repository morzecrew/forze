from typing import Any

import attrs

from forze.application.contracts.document import DocumentWritePort
from forze.application.execution import Usecase
from forze.application.mapping import DTOMapper
from forze.domain.models import BaseDTO, CreateDocumentCmd, ReadDocument

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class CreateDocument[In: BaseDTO, Cmd: CreateDocumentCmd, Out: ReadDocument](
    Usecase[In, Out]
):
    """Usecase that creates a new document from a mapped command.

    Maps the input DTO to a :class:`CreateDocumentCmd` via :attr:`mapper`, then
    delegates to :meth:`DocumentPort.create`. The mapper may inject fields such
    as ``number_id`` or ``creator_id`` using execution context.
    """

    doc: DocumentWritePort[Out, Any, Cmd, Any]
    """Document port for create operations."""

    mapper: DTOMapper[Cmd]
    """Mapper that converts input DTO to create command."""

    # ....................... #

    async def main(self, args: In) -> Out:
        """Create a document from the mapped command.

        :param args: Input DTO (e.g. request payload).
        :returns: Created read model.
        """
        cmd = await self.mapper(self.ctx, args)

        return await self.doc.create(cmd)
