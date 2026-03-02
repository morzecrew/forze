from typing import Any, TypedDict, final
from uuid import UUID

import attrs

from forze.application.contracts.document import DocumentPort
from forze.application.execution import Usecase
from forze.domain.models import ReadDocument

# ----------------------- #


@final
class SoftDeleteArgs(TypedDict):
    pk: UUID
    rev: int


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class KillDocument(Usecase[UUID, None]):
    doc: DocumentPort[Any, Any, Any, Any]

    # ....................... #

    async def main(self, args: UUID) -> None:
        return await self.doc.kill(args)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class DeleteDocument[Out: ReadDocument](Usecase[SoftDeleteArgs, Out]):
    doc: DocumentPort[Out, Any, Any, Any]

    # ....................... #

    async def main(self, args: SoftDeleteArgs) -> Out:
        return await self.doc.delete(args["pk"], rev=args.get("rev"))


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class RestoreDocument[Out: ReadDocument](Usecase[SoftDeleteArgs, Out]):
    doc: DocumentPort[Out, Any, Any, Any]

    # ....................... #

    async def main(self, args: SoftDeleteArgs) -> Out:
        return await self.doc.restore(args["pk"], rev=args.get("rev"))
