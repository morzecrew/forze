"""Buffer integration events and delegate durable flush to adapters."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from datetime import datetime
from typing import TYPE_CHECKING
from uuid import UUID

import attrs
from pydantic import BaseModel

from forze.application.contracts.outbox import (
    IntegrationEvent,
    OutboxSpec,
    StagedOutboxEntry,
)
from forze.base.exceptions import exc
from forze.base.primitives import utcnow, uuid7


if TYPE_CHECKING:
    from forze.application.execution.context import ExecutionContext
    from forze.application.execution.context import OutboxStagingContext

# ----------------------- #

FlushRowsFn = Callable[[Sequence[StagedOutboxEntry]], Awaitable[int]]

# ....................... #


@attrs.define(slots=True, kw_only=True)
class OutboxStaging[M: BaseModel]:
    """Request-scoped staging buffer with adapter-specific flush."""

    ctx: ExecutionContext
    """Active execution context."""

    spec: OutboxSpec[M]
    """Outbox route specification."""

    flush_rows: FlushRowsFn
    """Persist buffered rows; invoked by :meth:`flush`."""

    # ....................... #

    def _staging(self) -> OutboxStagingContext:
        return self.ctx.outbox_staging

    # ....................... #

    def _enrich_event(
        self,
        event_type: str,
        payload: M,
        *,
        event_id: UUID | None,
        occurred_at: datetime | None,
    ) -> IntegrationEvent[M]:
        event_id = event_id or uuid7()
        metadata = self.ctx.inv_ctx.get_metadata()
        tenant = self.ctx.inv_ctx.get_tenant()

        return IntegrationEvent(
            event_type=event_type,
            payload=payload,
            event_id=event_id,
            occurred_at=occurred_at or utcnow(),
            tenant_id=tenant.tenant_id if tenant is not None else None,
            execution_id=metadata.execution_id if metadata is not None else None,
            correlation_id=metadata.correlation_id if metadata is not None else None,
            causation_id=metadata.causation_id if metadata is not None else None,
        )

    # ....................... #

    def _to_entry(self, event: IntegrationEvent[M]) -> StagedOutboxEntry:
        return StagedOutboxEntry(
            outbox_route=str(self.spec.name),
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

        event = self._enrich_event(
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
        """Buffer a fully built integration event."""

        staging = self._staging()

        if staging.flushed:
            raise exc.internal("Cannot stage outbox events after flush")

        staging.buffer.push([self._to_entry(event)])

    # ....................... #

    async def flush(self) -> int:
        """Persist buffered events."""

        staging = self._staging()

        if staging.flushed:
            return 0

        rows = staging.buffer.pop()

        if not rows:
            staging.flushed = True
            return 0

        written = await self.flush_rows(rows)
        staging.flushed = True
        return written
