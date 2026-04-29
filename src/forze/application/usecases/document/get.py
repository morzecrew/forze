import attrs
from pydantic import BaseModel

from forze.application.contracts.document import DocumentQueryPort
from forze.application.dto import DocumentIdDTO, DocumentNumberIdDTO
from forze.application.execution import Usecase
from forze.base.errors import NotFoundError
from forze.domain.constants import NUMBER_ID_FIELD

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class GetDocument[R: BaseModel](Usecase[DocumentIdDTO, R]):
    """Usecase that fetches a single document by primary key.

    Delegates to :meth:`DocumentReadPort.get`. Read-only; uses the lighter
    :class:`DocumentReadPort`.
    """

    doc: DocumentQueryPort[R]
    """Document port for get operations."""

    # ....................... #

    async def main(self, args: DocumentIdDTO) -> R:
        """Fetch a document by primary key.

        :param args: Document primary key.
        :returns: Read model.
        """

        return await self.doc.get(pk=args.id)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class GetDocumentByNumberId[R: BaseModel](Usecase[DocumentNumberIdDTO, R]):
    """Usecase that fetches a single document by number ID."""

    doc: DocumentQueryPort[R]
    """Document port for get operations."""

    # ....................... #

    async def main(self, args: DocumentNumberIdDTO) -> R:
        """Fetch a document by number ID.

        :param args: Document number ID.
        :returns: Read model.
        """

        res = await self.doc.find(
            filters={
                "$fields": {NUMBER_ID_FIELD: args.number_id},
            }
        )

        if res is None:
            raise NotFoundError(f"Document not found with number ID: {args.number_id}")

        return res
