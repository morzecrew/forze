"""In-memory outbox store."""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable, Sequence
from datetime import datetime, timedelta
from typing import Any, final
from uuid import UUID

import attrs
from pydantic import BaseModel

from forze.application.contracts.outbox import (
    OutboxAdminPort,
    OutboxClaim,
    OutboxDepth,
    OutboxQueryPort,
    OutboxSpec,
    OutboxStatus,
    StagedOutboxEntry,
)
from forze.base.primitives import utcnow, uuid7
from forze_mock.adapters._journal import record_undo
from forze_mock.adapters.tx import ensure_mock_tx_writable
from forze_mock.state import MockState
from forze_mock.tenancy import MockTenancyMixin, partition_namespace

# ----------------------- #


def _remove_row(store: list[Any], row: Any) -> Callable[[], None]:
    """Build an undo thunk that removes *row* from *store* (no-op if already gone)."""

    def _undo() -> None:
        if row in store:
            store.remove(row)

    return _undo


# ....................... #

_TRANSITION_FIELDS = (
    "status",
    "processing_at",
    "published_at",
    "last_error",
    "attempts",
    "available_at",
)
"""Row fields mutated by status transitions (claim / mark / retry / requeue / reclaim)."""


def _restore_row(row: MockOutboxRow) -> Callable[[], None]:
    """Build an undo thunk restoring *row*'s transition fields to their current values.

    A status transition is a row UPDATE in production — Postgres rolls it back with the
    transaction — so one performed inside a mock transaction must revert on rollback too,
    exactly like the row append in :meth:`MockOutboxStore.persist_rows`. Per-row field
    restore (not a whole-store snapshot) keeps concurrent transactions' rows intact.
    Relay-side transitions on their own connection run outside any journal, where
    :func:`record_undo` is a no-op — those commit immediately, as before.
    """

    prior = {name: getattr(row, name) for name in _TRANSITION_FIELDS}

    def _undo() -> None:
        for name, value in prior.items():
            setattr(row, name, value)

    return _undo


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
    attempts: int = 0
    available_at: datetime | None = None
    ordering_key: str | None = None


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MockOutboxStore[M: BaseModel](MockTenancyMixin, OutboxQueryPort, OutboxAdminPort):
    """In-memory outbox persistence and query port."""

    spec: OutboxSpec[M]
    state: MockState

    def _route(self) -> str:
        return partition_namespace(
            self.require_tenant_if_aware(),
            str(self.spec.name),
        )

    # ....................... #

    async def persist_rows(self, rows: Sequence[StagedOutboxEntry]) -> int:
        # Outbox rows are DB rows in production: a strict read-only root rejects
        # the write (relay-side claim/mark never runs inside a request tx).
        ensure_mock_tx_writable(store=f"outbox:{self.spec.name}")

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

                row = MockOutboxRow(
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
                    ordering_key=event.ordering_key,
                )
                store.append(row)
                # Atomic with the business write: a rolled-back transaction removes exactly
                # this row (the row's uuid7 id makes the removal unambiguous), leaving
                # concurrent transactions' rows intact.
                record_undo(_remove_row(store, row))
                written += 1

        return written

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
                and (r.available_at is None or r.available_at <= now)
            ]
            pending.sort(key=lambda r: r.created_at)
            batch = pending[:max_n]

            for row in batch:
                record_undo(_restore_row(row))
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
                attempts=r.attempts,
                ordering_key=r.ordering_key,
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

    async def mark_retry(
        self,
        ids: Sequence[UUID],
        *,
        attempts: int,
        available_at: datetime,
        error: str | None = None,
    ) -> int:
        if not ids:
            return 0

        id_set = set(ids)
        route = self._route()
        updated = 0

        with self.state.lock:
            for row in self.state.outbox_rows.get(route, []):
                if row.id not in id_set:
                    continue

                if row.status != OutboxStatus.PROCESSING:
                    continue

                record_undo(_restore_row(row))
                row.status = OutboxStatus.PENDING
                row.processing_at = None
                row.attempts = attempts
                row.available_at = available_at
                row.last_error = error
                updated += 1

        return updated

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

                record_undo(_restore_row(row))
                row.status = OutboxStatus.PENDING
                row.processing_at = None
                reclaimed += 1

        return reclaimed

    async def requeue_failed(self, ids: Sequence[UUID]) -> int:
        if not ids:
            return 0

        id_set = set(ids)
        route = self._route()
        updated = 0

        with self.state.lock:
            for row in self.state.outbox_rows.get(route, []):
                if row.id not in id_set:
                    continue

                if row.status != OutboxStatus.FAILED:
                    continue

                record_undo(_restore_row(row))
                row.status = OutboxStatus.PENDING
                row.processing_at = None
                row.published_at = None
                row.last_error = None
                row.attempts = 0
                row.available_at = None
                updated += 1

        return updated

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

                record_undo(_restore_row(row))
                row.status = status

                if status == OutboxStatus.PUBLISHED:
                    row.published_at = now

                if status == OutboxStatus.FAILED:
                    row.last_error = error

                updated += 1

        return updated

    # ....................... #
    # Admin (observability) port

    def _rows(self) -> list[MockOutboxRow]:
        with self.state.lock:
            return list(self.state.outbox_rows.get(self._route(), []))

    # ....................... #

    async def has_undrained(self) -> bool:
        return any(
            row.status in (OutboxStatus.PENDING, OutboxStatus.PROCESSING) for row in self._rows()
        )

    # ....................... #

    async def depth(self) -> OutboxDepth:
        counts = Counter(row.status for row in self._rows())

        return OutboxDepth(
            pending=counts[OutboxStatus.PENDING],
            processing=counts[OutboxStatus.PROCESSING],
            failed=counts[OutboxStatus.FAILED],
        )

    # ....................... #

    async def oldest_pending_age(self) -> timedelta | None:
        pending = [row.created_at for row in self._rows() if row.status == OutboxStatus.PENDING]

        return utcnow() - min(pending) if pending else None
