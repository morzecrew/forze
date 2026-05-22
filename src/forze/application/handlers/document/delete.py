from typing import Any

import attrs

from forze.application.contracts.document import DocumentCommandPort
from forze.application.dto import DocumentIdDTO
from forze.application.execution.core import Handler

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class KillDocument(Handler[DocumentIdDTO, None]):
    """Handler that permanently deletes a document (hard delete)."""

    doc: DocumentCommandPort[Any, Any, Any, Any]
    """Document port for kill operations."""

    # ....................... #

    async def __call__(self, args: DocumentIdDTO) -> None:
        """Permanently delete a document.

        :param args: Document primary key.
        :returns: ``None``.
        """

        return await self.doc.kill(pk=args.id)
