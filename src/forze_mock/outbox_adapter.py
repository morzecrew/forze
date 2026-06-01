"""In-memory outbox command and query adapters."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from typing import Any, final
from uuid import UUID

import attrs
from pydantic import BaseModel

from forze.application.contracts.outbox import (
    IntegrationEvent,
    OutboxClaim,
    OutboxCommandPort,
    OutboxQueryPort,
    OutboxSpec,
    OutboxStatus,
    StagedOutboxEntry,
)
from forze.application.execution.context import ExecutionContext
from forze.application.integrations.outbox import OutboxStaging
from forze.base.primitives import utcnow, uuid7
from forze_mock.state import MockState
from forze_mock.tenancy import MockTenancyMixin, partition_namespace

# ----------------------- #


@final
@attrs.define(slots=True)
class MockOutboxRow:
    """Stored outbox row in :class:`MockState`."""

    id: UUID
    outbox_route: str
    event_id: UUID
    event_type: str
    payload: dict[str, Any]
    status: OutboxStatus
    tenant_id: UUID | None
    execution_id: UUID | None
    correlation_id: UUID | None
    causation_id: UUID | None
    occurred_at: datetime
    created_at: datetime
    published_at: datetime | None = None
    processing_at: datetime | None = None
    last_error: str | None = None


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MockOutboxAdapter[M: BaseModel](
    MockTenancyMixin,
    OutboxCommandPort[M],
    OutboxQueryPort,
):
    """In-memory outbox staging, flush, claim, and mark adapters."""

    ctx: ExecutionContext
    spec: OutboxSpec[M]
    state: MockState

    def _route(self) -> str:
        return partition_namespace(
            self.require_tenant_if_aware(),
            str(self.spec.name),
        )

    _staging: OutboxStaging[M] = attrs.field(init=False)

    def __attrs_post_init__(self) -> None:
        object.__setattr__(
            self,
            "_staging",
            OutboxStaging(
                ctx=self.ctx,
                spec=self.spec,
                flush_rows=self._persist_rows,
            ),
        )

    # ....................... #

    async def _persist_rows(self, rows: Sequence[StagedOutboxEntry]) -> int:
        route = self._route()
        written = 0

        with self.state.lock:
            store = self.state.outbox_rows.setdefault(route, [])

            for entry in rows:
                event = entry.event
                existing = next(
                    (r for r in store if r.event_id == event.event_id),
                    None,
                )

                if existing is not None:
                    continue

                store.append(
                    MockOutboxRow(
                        id=uuid7(),
                        outbox_route=route,
                        event_id=event.event_id,
                        event_type=event.event_type,
                        payload=dict(entry.payload_json),
                        status=OutboxStatus.PENDING,
                        tenant_id=event.tenant_id,
                        execution_id=event.execution_id,
                        correlation_id=event.correlation_id,
                        causation_id=event.causation_id,
                        occurred_at=event.occurred_at,
                        created_at=utcnow(),
                    )
                )
                written += 1

        return written

    # ....................... #

    async def stage(
        self,
        event_type: str,
        payload: M,
        *,
        event_id: UUID | None = None,
        occurred_at: datetime | None = None,
    ) -> None:
        await self._staging.stage(
            event_type,
            payload,
            event_id=event_id,
            occurred_at=occurred_at,
        )

    async def stage_many(
        self,
        events: Sequence[tuple[str, M]],
        *,
        event_ids: Sequence[UUID] | None = None,
    ) -> None:
        await self._staging.stage_many(events, event_ids=event_ids)

    async def stage_event(self, event: IntegrationEvent[M]) -> None:
        await self._staging.stage_event(event)

    async def flush(self) -> int:
        return await self._staging.flush()

    # ....................... #

    async def claim_pending(
        self,
        *,
        limit: int | None = None,
    ) -> Sequence[OutboxClaim]:
        route = self._route()
        max_n = limit if limit is not None else 100
        now = utcnow()

        with self.state.lock:
            pending = [
                r
                for r in self.state.outbox_rows.get(route, [])
                if r.status == OutboxStatus.PENDING
            ]
            pending.sort(key=lambda r: r.created_at)
            batch = pending[:max_n]

            for row in batch:
                row.status = OutboxStatus.PROCESSING
                row.processing_at = now

        return [
            OutboxClaim(
                id=r.id,
                outbox_route=r.outbox_route,
                event_id=r.event_id,
                event_type=r.event_type,
                payload=dict(r.payload),
                tenant_id=r.tenant_id,
                execution_id=r.execution_id,
                correlation_id=r.correlation_id,
                causation_id=r.causation_id,
                occurred_at=r.occurred_at,
            )
            for r in batch
        ]

    async def mark_published(self, ids: Sequence[UUID]) -> int:
        return self._mark(ids, OutboxStatus.PUBLISHED)

    async def mark_failed(
        self,
        ids: Sequence[UUID],
        *,
        error: str | None = None,
    ) -> int:
        return self._mark(ids, OutboxStatus.FAILED, error=error)

    async def reclaim_stale_processing(
        self,
        *,
        older_than: datetime,
    ) -> int:
        route = self._route()
        reclaimed = 0

        with self.state.lock:
            for row in self.state.outbox_rows.get(route, []):
                if row.status != OutboxStatus.PROCESSING:
                    continue

                if row.processing_at is None or row.processing_at >= older_than:
                    continue

                row.status = OutboxStatus.PENDING
                row.processing_at = None
                reclaimed += 1

        return reclaimed

    def _mark(
        self,
        ids: Sequence[UUID],
        status: OutboxStatus,
        *,
        error: str | None = None,
    ) -> int:
        if not ids:
            return 0

        id_set = set(ids)
        route = self._route()
        updated = 0
        now = utcnow()

        with self.state.lock:
            for row in self.state.outbox_rows.get(route, []):
                if row.id not in id_set:
                    continue

                if row.status != OutboxStatus.PROCESSING:
                    continue

                row.status = status

                if status == OutboxStatus.PUBLISHED:
                    row.published_at = now

                if status == OutboxStatus.FAILED:
                    row.last_error = error

                updated += 1

        return updated
