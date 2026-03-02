from typing import Any, TypedDict, final
from uuid import UUID

import attrs

from forze.application.contracts.document import DocumentPort
from forze.application.execution import Usecase
from forze.application.mapping import DTOMapper
from forze.domain.models import BaseDTO, ReadDocument

# ----------------------- #


@final
class UpdateArgs[In: BaseDTO](TypedDict):
    pk: UUID
    dto: In
    rev: int


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class UpdateDocument[In: BaseDTO, Cmd: BaseDTO, Out: ReadDocument](
    Usecase[UpdateArgs[In], Out]
):
    doc: DocumentPort[Out, Any, Any, Cmd]
    mapper: DTOMapper[Cmd]

    # ....................... #

    async def main(self, args: UpdateArgs[In]) -> Out:
        cmd = await self.mapper(self.ctx, args["dto"])

        return await self.doc.update(args["pk"], cmd, rev=args.get("rev"))
