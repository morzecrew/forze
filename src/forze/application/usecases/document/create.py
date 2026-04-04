from typing import Any

import attrs

from forze.application.contracts.document import DocumentCommandPort
from forze.application.contracts.mapping import MapperPort
from forze.application.execution import Usecase
from forze.domain.models import BaseDTO, CreateDocumentCmd, ReadDocument

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class CreateDocument[In: BaseDTO, Cmd: CreateDocumentCmd, Out: ReadDocument](
    Usecase[In, Out]
):
    """Usecase that creates a new document from a mapped command."""

    doc: DocumentCommandPort[Out, Any, Cmd, Any]
    """Document port for create operations."""

    mapper: MapperPort[In, Cmd]
    """Mapper that converts input DTO to create command."""

    # ....................... #

    async def main(self, args: In) -> Out:
        """Create a document from the mapped command.

        :param args: Input DTO (e.g. request payload).
        :returns: Created read model.
        """

        cmd = await self.mapper(args, ctx=self.ctx)

        return await self.doc.create(dto=cmd)
