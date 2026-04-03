from typing import Any

import attrs

from forze.application.contracts.document import DocumentWritePort
from forze.application.contracts.mapping import MapperPort
from forze.application.dto import DocumentUpdateDTO
from forze.application.execution import Usecase
from forze.domain.models import BaseDTO, ReadDocument

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class UpdateDocument[In: BaseDTO, Cmd: BaseDTO, Out: ReadDocument](
    Usecase[DocumentUpdateDTO[In], Out]
):
    """Usecase that updates an existing document from a mapped command."""

    doc: DocumentWritePort[Out, Any, Any, Cmd]
    """Document port for update operations."""

    mapper: MapperPort[In, Cmd]
    """Mapper that converts input DTO to update command."""

    # ....................... #

    async def main(self, args: DocumentUpdateDTO[In]) -> Out:
        """Update a document from the mapped command.

        :param args: Update arguments (pk, dto, rev).
        :returns: Updated read model.
        """

        cmd = await self.mapper(args.dto, ctx=self.ctx)

        return await self.doc.update(pk=args.id, rev=args.rev, dto=cmd)
