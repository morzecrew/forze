from typing import Any

import attrs

from forze.application.contracts.document import DocumentPort
from forze.application.execution import Usecase
from forze.application.mapping import DTOMapper
from forze.domain.models import BaseDTO, CreateDocumentCmd, ReadDocument

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class CreateDocument[In: BaseDTO, Cmd: CreateDocumentCmd, Out: ReadDocument](
    Usecase[In, Out]
):
    doc: DocumentPort[Out, Any, Cmd, Any]
    mapper: DTOMapper[Cmd]

    # ....................... #

    async def main(self, args: In) -> Out:
        cmd = await self.mapper(self.ctx, args)

        return await self.doc.create(cmd)
