from typing import Any

import attrs
from pydantic import BaseModel as BM

from forze.application.contracts.document import DocumentCommandPort
from forze.application.contracts.mapping import Mapper
from forze.application.dto import DocumentUpdateDTO, DocumentUpdateRes
from forze.application.execution.core import Handler
from forze.domain.models import BaseDTO

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class UpdateDocument[In: BaseDTO, Cmd: BaseDTO, Out: BM](
    Handler[DocumentUpdateDTO[In], DocumentUpdateRes[Out]]
):
    """Usecase that updates an existing document from a mapped command and returns a result with diff."""

    doc: DocumentCommandPort[Out, Any, Any, Cmd]
    """Document port for update operations."""

    mapper: Mapper[In, Cmd]
    """Mapper that converts input DTO to update command."""

    # ....................... #

    async def __call__(self, args: DocumentUpdateDTO[In]) -> DocumentUpdateRes[Out]:
        """Update a document from the mapped command and return a result with diff.

        :param args: Update arguments (pk, dto, rev).
        :returns: Updated read model and diff.
        """

        cmd = await self.mapper(args.dto)

        res, diff = await self.doc.update(
            pk=args.id,
            rev=args.rev,
            dto=cmd,
            return_diff=True,
        )

        return DocumentUpdateRes(data=res, diff=diff)
