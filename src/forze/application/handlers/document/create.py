from typing import Any

import attrs
from pydantic import BaseModel as BM

from forze.application.contracts.document import DocumentCommandPort
from forze.application.contracts.mapping import Mapper
from forze.application.execution import Handler
from forze.domain.models import BaseDTO as BDTO
from forze.domain.models import CreateDocumentCmd as CDC

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class CreateDocument[In: BDTO, Cmd: CDC, Out: BM](Handler[In, Out]):
    """Handler that creates a new document from a mapped command."""

    doc: DocumentCommandPort[Out, Any, Cmd, Any]
    """Document port for create operations."""

    mapper: Mapper[In, Cmd]
    """Mapper that converts input DTO to create command."""

    # ....................... #

    async def main(self, args: In) -> Out:
        """Create a document from the mapped command.

        :param args: Input DTO (e.g. request payload).
        :returns: Created read model.
        """

        cmd = await self.mapper(args)

        return await self.doc.create(dto=cmd)
