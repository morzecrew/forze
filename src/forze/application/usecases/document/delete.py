from typing import Any, TypedDict, final
from uuid import UUID

import attrs

from forze.application.contracts.document import DocumentPort
from forze.application.execution import Usecase
from forze.domain.models import ReadDocument

# ----------------------- #


@final
class SoftDeleteArgs(TypedDict):
    """Arguments for soft delete and restore usecases."""

    pk: UUID
    """Document primary key."""

    rev: int
    """Expected revision for optimistic concurrency (optional)."""


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class KillDocument(Usecase[UUID, None]):
    """Usecase that permanently deletes a document (hard delete).

    Delegates to :meth:`DocumentPort.kill`. Irreversible; use
    :class:`DeleteDocument` for soft delete when possible.
    """

    doc: DocumentPort[Any, Any, Any, Any]
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
    """Usecase that soft-deletes a document.

    Delegates to :meth:`DocumentPort.delete`. Returns the updated read model
    with deletion metadata. Supports optimistic concurrency via ``rev``.
    """

    doc: DocumentPort[Out, Any, Any, Any]
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
    """Usecase that restores a soft-deleted document.

    Delegates to :meth:`DocumentPort.restore`. Returns the updated read model.
    Supports optimistic concurrency via ``rev``.
    """

    doc: DocumentPort[Out, Any, Any, Any]
    """Document port for restore operations."""

    # ....................... #

    async def main(self, args: SoftDeleteArgs) -> Out:
        """Restore a soft-deleted document.

        :param args: Restore arguments (pk, optional rev).
        :returns: Updated read model.
        """
        return await self.doc.restore(args["pk"], rev=args.get("rev"))
