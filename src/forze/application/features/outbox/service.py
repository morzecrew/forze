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
    doc: DocumentWritePort[
        ReadOutboxEvent, OutboxEvent, CreateOutboxEventCmd, UpdateOutboxEventCmd
    ]

    # ....................... #

    async def append(self, draft: CreateOutboxEventCmd) -> ReadOutboxEvent:
        return await self.doc.create(draft)

    # ....................... #

    async def append_many(
        self,
        drafts: Sequence[CreateOutboxEventCmd],
    ) -> Sequence[ReadOutboxEvent]:
        return await self.doc.create_many(drafts)

    # ....................... #

    async def mark_as_published(
        self,
        pk: UUID,
        *,
        rev: int,
        published_at: Optional[datetime] = None,
    ) -> ReadOutboxEvent:
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
        published_at = published_at or utcnow()
        cmds = [UpdateOutboxEventCmd(published_at=published_at) for _ in pks]
        return await self.doc.update_many(pks, cmds, revs=revs)
