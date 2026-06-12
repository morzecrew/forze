"""Outbox command port backed by request-scoped staging."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from typing import final
from uuid import UUID

import attrs
from pydantic import BaseModel

from forze.application.contracts.outbox import IntegrationEvent, OutboxSpec

from .staging import OutboxStaging

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class StagingOutboxCommand[M: BaseModel]:
    """Implements :class:`~forze.application.contracts.outbox.OutboxCommandPort` via staging."""

    spec: OutboxSpec[M]
    """Outbox specification for this command port."""

    staging: OutboxStaging[M]
    """Request-scoped staging buffer."""

    # ....................... #

    async def stage(
        self,
        event_type: str,
        payload: M,
        *,
        event_id: UUID | None = None,
        occurred_at: datetime | None = None,
        ordering_key: str | None = None,
    ) -> None:
        await self.staging.stage(
            event_type,
            payload,
            event_id=event_id,
            occurred_at=occurred_at,
            ordering_key=ordering_key,
        )

    # ....................... #

    async def stage_many(
        self,
        events: Sequence[tuple[str, M]],
        *,
        event_ids: Sequence[UUID] | None = None,
    ) -> None:
        await self.staging.stage_many(events, event_ids=event_ids)

    # ....................... #

    async def stage_event(self, event: IntegrationEvent[M]) -> None:
        await self.staging.stage_event(event)

    # ....................... #

    async def flush(self) -> int:
        return await self.staging.flush()
