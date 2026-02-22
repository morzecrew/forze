from typing import Any, Callable

import attrs

from forze.application.dto.internal import UpdateArgs
from forze.application.kernel.ports import DocumentPort
from forze.application.kernel.usecase import TxUsecase
from forze.domain.models import BaseDTO, ReadDocument

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class UpdateDocument[In: BaseDTO, Cmd: BaseDTO, Out: ReadDocument](
    TxUsecase[UpdateArgs[In], Out]
):
    doc: DocumentPort[Out, Any, Any, Cmd]
    mapper: Callable[[In], Cmd]

    # ....................... #

    async def main(self, args: UpdateArgs[In]) -> Out:
        cmd = self.mapper(args["dto"])

        return await self.doc.update(args["pk"], cmd, rev=args.get("rev"))
