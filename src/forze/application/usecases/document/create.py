from typing import Any, Callable

import attrs

from forze.application.kernel.ports import CounterPort, DocumentPort
from forze.application.kernel.usecase import TxUsecase
from forze.domain.models import BaseDTO, CreateDocumentCmd, ReadDocument

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class CreateDocument[In: BaseDTO, Cmd: CreateDocumentCmd, Out: ReadDocument](
    TxUsecase[In, Out]
):
    doc: DocumentPort[Out, Any, Cmd, Any]
    mapper: Callable[[In], Cmd]

    # ....................... #

    async def main(self, args: In) -> Out:
        cmd = self.mapper(args)

        return await self.doc.create(cmd)


# ....................... #
# Numbered


@attrs.define(slots=True, kw_only=True, frozen=True)
class CreateNumberedDocument[In: BaseDTO, Cmd: CreateDocumentCmd, Out: ReadDocument](
    TxUsecase[In, Out]
):
    doc: DocumentPort[Out, Any, Cmd, Any]
    counter: CounterPort
    mapper: Callable[[In, int], Cmd]

    # ....................... #

    async def main(self, args: In) -> Out:
        number_id = await self.counter.incr()
        cmd = self.mapper(args, number_id)

        return await self.doc.create(cmd)
