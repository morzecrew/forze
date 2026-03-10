from datetime import datetime
from typing import Optional, Sequence, final
from uuid import UUID

import attrs

from forze.application.contracts.document import DocumentWritePort
from forze.base.primitives import utcnow

from .model import (
    CreateOutboxEventCmd,
    OutboxEvent,
    ReadOutboxEvent,
    UpdateOutboxEventCmd,
)

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class OutboxService:
    """Application service for appending and marking outbox events as published."""

    doc: DocumentWritePort[
        ReadOutboxEvent, OutboxEvent, CreateOutboxEventCmd, UpdateOutboxEventCmd
    ]
    """Underlying document write port for outbox persistence."""

    # ....................... #

    async def append(self, draft: CreateOutboxEventCmd) -> ReadOutboxEvent:
        """Create a single outbox event from *draft*."""

        return await self.doc.create(draft)

    # ....................... #

    async def append_many(
        self,
        drafts: Sequence[CreateOutboxEventCmd],
    ) -> Sequence[ReadOutboxEvent]:
        """Create multiple outbox events in bulk."""

        return await self.doc.create_many(drafts)

    # ....................... #

    async def mark_as_published(
        self,
        pk: UUID,
        *,
        rev: int,
        published_at: Optional[datetime] = None,
    ) -> ReadOutboxEvent:
        """Mark a single outbox event as published with an optimistic-lock rev check."""

        published_at = published_at or utcnow()
        cmd = UpdateOutboxEventCmd(published_at=published_at)

        return await self.doc.update(pk, cmd, rev=rev)

    # ....................... #

    async def mark_many_as_published(
        self,
        pks: Sequence[UUID],
        *,
        revs: Sequence[int],
        published_at: Optional[datetime] = None,
    ) -> Sequence[ReadOutboxEvent]:
        """Mark multiple outbox events as published in bulk."""

        published_at = published_at or utcnow()
        cmds = [UpdateOutboxEventCmd(published_at=published_at) for _ in pks]
        return await self.doc.update_many(pks, cmds, revs=revs)
