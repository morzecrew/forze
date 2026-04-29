from typing import Any

import attrs
from pydantic import BaseModel

from forze.application.contracts.document import DocumentCommandPort
from forze.application.dto import DocumentIdDTO, DocumentIdRevDTO
from forze.application.execution import Usecase

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class KillDocument(Usecase[DocumentIdDTO, None]):
    """Usecase that permanently deletes a document (hard delete)."""

    doc: DocumentCommandPort[Any, Any, Any, Any]
    """Document port for kill operations."""

    # ....................... #

    async def main(self, args: DocumentIdDTO) -> None:
        """Permanently delete a document.

        :param args: Document primary key.
        :returns: ``None``.
        """

        return await self.doc.kill(pk=args.id)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class DeleteDocument[Out: BaseModel](Usecase[DocumentIdRevDTO, Out]):
    """Usecase that soft-deletes a document."""

    doc: DocumentCommandPort[Out, Any, Any, Any]
    """Document port for delete operations."""

    # ....................... #

    async def main(self, args: DocumentIdRevDTO) -> Out:
        """Soft-delete a document.

        :param args: Delete arguments (pk, optional rev).
        :returns: Updated read model.
        """

        return await self.doc.delete(pk=args.id, rev=args.rev)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class RestoreDocument[Out: BaseModel](Usecase[DocumentIdRevDTO, Out]):
    """Usecase that restores a soft-deleted document."""

    doc: DocumentCommandPort[Out, Any, Any, Any]
    """Document port for restore operations."""

    # ....................... #

    async def main(self, args: DocumentIdRevDTO) -> Out:
        """Restore a soft-deleted document.

        :param args: Restore arguments (pk, optional rev).
        :returns: Updated read model.
        """

        return await self.doc.restore(pk=args.id, rev=args.rev)
