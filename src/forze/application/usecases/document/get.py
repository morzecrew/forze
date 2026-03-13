from uuid import UUID

import attrs

from forze.application.contracts.document import DocumentReadPort
from forze.application.execution import Usecase
from forze.base.logging import getLogger
from forze.domain.models import ReadDocument

# ----------------------- #

logger = getLogger(__name__)


@attrs.define(slots=True, kw_only=True, frozen=True)
class GetDocument[Out: ReadDocument](Usecase[UUID, Out]):
    """Usecase that fetches a single document by primary key.

    Delegates to :meth:`DocumentReadPort.get`. Read-only; uses the lighter
    :class:`DocumentReadPort`.
    """

    doc: DocumentReadPort[Out]
    """Read-only document port for get operations."""

    # ....................... #

    async def main(self, args: UUID) -> Out:
        """Fetch a document by primary key.

        :param args: Document primary key.
        :returns: Read model.
        """

        return await self.doc.get(args)
