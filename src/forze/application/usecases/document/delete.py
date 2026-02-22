from typing import Any
from uuid import UUID

import attrs

from forze.application.dto.internal import SoftDeleteArgs
from forze.application.kernel.ports import DocumentPort
from forze.application.kernel.usecase import TxUsecase
from forze.domain.models import ReadDocument

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class KillDocument(TxUsecase[UUID, None]):
    doc: DocumentPort[Any, Any, Any, Any]

    # ....................... #

    async def main(self, args: UUID) -> None:
        return await self.doc.kill(args)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class DeleteDocument[Out: ReadDocument](TxUsecase[SoftDeleteArgs, Out]):
    doc: DocumentPort[Out, Any, Any, Any]

    # ....................... #

    async def main(self, args: SoftDeleteArgs) -> Out:
        return await self.doc.delete(args["pk"], rev=args.get("rev"))


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class RestoreDocument[Out: ReadDocument](TxUsecase[SoftDeleteArgs, Out]):
    doc: DocumentPort[Out, Any, Any, Any]

    # ....................... #

    async def main(self, args: SoftDeleteArgs) -> Out:
        return await self.doc.restore(args["pk"], rev=args.get("rev"))
