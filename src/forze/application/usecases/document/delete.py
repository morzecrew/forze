from typing import Any, TypedDict, final
from uuid import UUID

import attrs

from forze.application.contracts.document import DocumentWritePort
from forze.application.execution import Usecase
from forze.domain.models import ReadDocument

# ----------------------- #


@final  #! TODO: replace with BaseDTO
class SoftDeleteArgs(TypedDict):
    """Arguments for soft delete and restore usecases."""

    pk: UUID
    """Document primary key."""

    rev: int
    """Expected revision for optimistic concurrency (optional)."""


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class KillDocument(Usecase[UUID, None]):
    """Usecase that permanently deletes a document (hard delete)."""

    doc: DocumentWritePort[Any, Any, Any, Any]
    """Document port for kill operations."""

    # ....................... #

    async def main(self, args: UUID) -> None:
        """Permanently delete a document.

        :param args: Document primary key.
        :returns: ``None``.
        """

        return await self.doc.kill(args)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class DeleteDocument[Out: ReadDocument](Usecase[SoftDeleteArgs, Out]):
    """Usecase that soft-deletes a document."""

    doc: DocumentWritePort[Out, Any, Any, Any]
    """Document port for delete operations."""

    # ....................... #

    async def main(self, args: SoftDeleteArgs) -> Out:
        """Soft-delete a document.

        :param args: Delete arguments (pk, optional rev).
        :returns: Updated read model.
        """

        return await self.doc.delete(args["pk"], rev=args.get("rev"))


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class RestoreDocument[Out: ReadDocument](Usecase[SoftDeleteArgs, Out]):
    """Usecase that restores a soft-deleted document."""

    doc: DocumentWritePort[Out, Any, Any, Any]
    """Document port for restore operations."""

    # ....................... #

    async def main(self, args: SoftDeleteArgs) -> Out:
        """Restore a soft-deleted document.

        :param args: Restore arguments (pk, optional rev).
        :returns: Updated read model.
        """

        return await self.doc.restore(args["pk"], rev=args.get("rev"))
