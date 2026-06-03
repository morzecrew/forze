from typing import Any, cast

import attrs
from pydantic import BaseModel as BM

from forze.application.contracts.document import DocumentCommandPort
from forze.application.contracts.execution import Handler
from forze_kits.aggregates.document.dto import DocumentIdRevDTO
from forze_kits.domain.soft_deletion.models import (
    DocWithSoftDeletion,
    UpdateCmdWithSoftDeletion,
)

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class DeleteDocument[Out: BM, D: DocWithSoftDeletion, U: UpdateCmdWithSoftDeletion](
    Handler[DocumentIdRevDTO, Out]
):
    """Handler that soft-deletes a document."""

    doc: DocumentCommandPort[Out, D, Any, U]
    """Document port for delete operations."""

    # ....................... #

    async def __call__(self, args: DocumentIdRevDTO) -> Out:
        """Soft-delete a document.

        :param args: Delete arguments (pk, optional rev).
        :returns: Updated read model.
        """

        upd_cls = cast(type[U], UpdateCmdWithSoftDeletion)

        return await self.doc.update(
            pk=args.id,
            rev=args.rev,
            dto=upd_cls(is_deleted=True),
        )


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class RestoreDocument[Out: BM, D: DocWithSoftDeletion, U: UpdateCmdWithSoftDeletion](
    Handler[DocumentIdRevDTO, Out]
):
    """Handler that restores a soft-deleted document."""

    doc: DocumentCommandPort[Out, D, Any, U]
    """Document port for restore operations."""

    # ....................... #

    async def __call__(self, args: DocumentIdRevDTO) -> Out:
        """Restore a soft-deleted document.

        :param args: Restore arguments (pk, optional rev).
        :returns: Updated read model.
        """

        upd_cls = cast(type[U], UpdateCmdWithSoftDeletion)

        return await self.doc.update(
            pk=args.id,
            rev=args.rev,
            dto=upd_cls(is_deleted=False),
        )
