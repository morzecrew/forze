import attrs

from forze.application.contracts.document import DocumentReadPort
from forze.application.dto import DocumentIdDTO
from forze.application.execution import Usecase
from forze.domain.models import ReadDocument

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class GetDocument[R: ReadDocument](Usecase[DocumentIdDTO, R]):
    """Usecase that fetches a single document by primary key.

    Delegates to :meth:`DocumentReadPort.get`. Read-only; uses the lighter
    :class:`DocumentReadPort`.
    """

    doc: DocumentReadPort[R]
    """Read-only document port for get operations."""

    # ....................... #

    async def main(self, args: DocumentIdDTO) -> R:
        """Fetch a document by primary key.

        :param args: Document primary key.
        :returns: Read model.
        """

        return await self.doc.get(args.id)
