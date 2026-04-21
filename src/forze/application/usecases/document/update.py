from typing import Any

import attrs

from forze.application.contracts.document import DocumentCommandPort
from forze.application.contracts.mapping import MapperPort
from forze.application.dto import DocumentUpdateDTO, DocumentUpdateRes
from forze.application.execution import Usecase
from forze.domain.models import BaseDTO, ReadDocument

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class UpdateDocument[In: BaseDTO, Cmd: BaseDTO, Out: ReadDocument](
    Usecase[DocumentUpdateDTO[In], DocumentUpdateRes[Out]]
):
    """Usecase that updates an existing document from a mapped command and returns a result with diff."""

    doc: DocumentCommandPort[Out, Any, Any, Cmd]
    """Document port for update operations."""

    mapper: MapperPort[In, Cmd]
    """Mapper that converts input DTO to update command."""

    # ....................... #

    async def main(self, args: DocumentUpdateDTO[In]) -> DocumentUpdateRes[Out]:
        """Update a document from the mapped command and return a result with diff.

        :param args: Update arguments (pk, dto, rev).
        :returns: Updated read model and diff.
        """

        cmd = await self.mapper(args.dto, ctx=self.ctx)

        res, diff = await self.doc.update(
            pk=args.id,
            rev=args.rev,
            dto=cmd,
            return_diff=True,
        )

        return DocumentUpdateRes(data=res, diff=diff)
