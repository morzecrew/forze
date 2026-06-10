"""Buffer integration events and delegate durable flush to stores."""

from collections.abc import Awaitable, Callable, Sequence
from datetime import datetime
from uuid import UUID

import attrs
from pydantic import BaseModel

from forze.application.contracts.outbox import (
    IntegrationEvent,
    OutboxSpec,
    StagedOutboxEntry,
)
from forze.application.contracts.outbox.staging_context import OutboxStagingContext
from forze.base.exceptions import exc

from .enrichment import OutboxEventEnricher

# ----------------------- #

FlushRowsFn = Callable[[Sequence[StagedOutboxEntry]], Awaitable[int]]

# ....................... #


@attrs.define(slots=True, kw_only=True)
class OutboxStaging[M: BaseModel]:
    """Request-scoped staging buffer with store-specific flush."""

    staging: OutboxStagingContext
    """Per-request buffer and flush flag."""

    spec: OutboxSpec[M]
    """Outbox route specification."""

    enricher: OutboxEventEnricher
    """Builds integration events with invocation envelope fields."""

    flush_rows: FlushRowsFn
    """Persist buffered rows; invoked by :meth:`flush`."""

    # ....................... #

    @property
    def _route(self) -> str:
        return str(self.spec.name)

    # ....................... #

    def _to_entry(self, event: IntegrationEvent[M]) -> StagedOutboxEntry:
        return StagedOutboxEntry(
            outbox_route=self._route,
            event=event,
            payload_json=self.spec.codec.encode_mapping(event.payload),
        )

    # ....................... #

    async def stage(
        self,
        event_type: str,
        payload: M,
        *,
        event_id: UUID | None = None,
        occurred_at: datetime | None = None,
    ) -> None:
        """Buffer an integration event."""

        event = self.enricher.enrich(
            event_type,
            payload,
            event_id=event_id,
            occurred_at=occurred_at,
        )
        await self.stage_event(event)

    # ....................... #

    async def stage_many(
        self,
        events: Sequence[tuple[str, M]],
        *,
        event_ids: Sequence[UUID] | None = None,
    ) -> None:
        """Buffer multiple integration events."""

        if event_ids is not None and len(event_ids) != len(events):
            raise exc.precondition("event_ids length must match events length")

        for index, (event_type, payload) in enumerate(events):
            eid = event_ids[index] if event_ids is not None else None
            await self.stage(event_type, payload, event_id=eid)

    # ....................... #

    async def stage_event(self, event: IntegrationEvent[M]) -> None:
        """Buffer a fully built integration event into this spec's route."""

        route = self._route

        if self.staging.flushed_for(route):
            raise exc.internal("Cannot stage outbox events after flush")

        self.staging.buffer_for(route).push([self._to_entry(event)])

    # ....................... #

    async def flush(self) -> int:
        """Persist events buffered for this spec's route only."""

        route = self._route

        if self.staging.flushed_for(route):
            return 0

        rows = self.staging.buffer_for(route).pop()

        if not rows:
            self.staging.set_flushed(route, True)
            return 0

        written = await self.flush_rows(rows)
        self.staging.set_flushed(route, True)
        return written
