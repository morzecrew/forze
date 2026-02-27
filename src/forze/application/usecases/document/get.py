from uuid import UUID

import attrs

from forze.application.contracts.document import DocumentReadPort
from forze.application.execution import Usecase
from forze.domain.models import ReadDocument

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class GetDocument[Out: ReadDocument](Usecase[UUID, Out]):
    doc: DocumentReadPort[Out]

    # ....................... #

    async def main(self, args: UUID) -> Out:
        return await self.doc.get(args)
