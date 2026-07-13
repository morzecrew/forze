"""Outbox command and query port protocols."""

from collections.abc import Awaitable, Sequence
from datetime import datetime
from typing import Any, Protocol, runtime_checkable
from uuid import UUID

from pydantic import BaseModel

from .specs import OutboxSpec
from .value_objects import IntegrationEvent, OutboxClaim, StagedOutboxEntry

# ----------------------- #


@runtime_checkable
class OutboxRowPersistPort(Protocol):
    """Narrow store surface used when wiring flush into staging."""

    def persist_rows(self, rows: Sequence[StagedOutboxEntry]) -> Awaitable[int]:
        """Insert staged rows; return count of new rows."""
        ...


# ....................... #


@runtime_checkable
class OutboxCommandPort[M: BaseModel](Protocol):
    """Stage integration events and flush them durably (typically in the active transaction)."""

    spec: OutboxSpec[M]
    """Outbox specification for this port instance."""

    def stage(
        self,
        event_type: str,
        payload: M,
        *,
        event_id: UUID | None = None,
        occurred_at: datetime | None = None,
        ordering_key: str | None = None,
    ) -> Awaitable[None]:
        """Buffer an integration event for a later :meth:`flush`.

        *ordering_key* partitions delivery on capable transports (SQS FIFO
        ``MessageGroupId``, stream partition key): same-key events relay in
        ``created_at`` order on the happy path. It does **not** guarantee
        ordering — a retrying/failed row never stalls later rows of its key.
        """
        ...  # pragma: no cover

    def stage_many(
        self,
        events: Sequence[tuple[str, M]],
        *,
        event_ids: Sequence[UUID] | None = None,
    ) -> Awaitable[None]:
        """Buffer multiple integration events for a later :meth:`flush`."""
        ...  # pragma: no cover

    def stage_event(self, event: IntegrationEvent[M]) -> Awaitable[None]:
        """Buffer a fully built :class:`~.IntegrationEvent`."""
        ...  # pragma: no cover

    def flush(self) -> Awaitable[int]:
        """Persist buffered events and return the number of new rows inserted."""
        ...  # pragma: no cover


# ....................... #


@runtime_checkable
class OutboxQueryPort(Protocol):
    """Claim and update staged outbox rows for relay workers.

    Delivery through the outbox is **at-least-once**, and ordering is **not**
    preserved across failures and retries: a row rescheduled with
    :meth:`mark_retry` (or parked with :meth:`mark_failed`) does not stall
    later rows of the same route or aggregate — deliberately, so one poison
    row never head-of-line blocks its key. ``claim_pending`` returns rows in
    ``created_at`` order, which preserves same-``ordering_key`` order on the
    happy path. Consumers must dedup on ``event_id`` and tolerate both
    redelivery and reordering.
    """

    spec: OutboxSpec[Any]
    """Outbox specification for this port instance."""

    def claim_pending(
        self,
        *,
        limit: int | None = None,
    ) -> Awaitable[Sequence[OutboxClaim]]:
        """Claim a batch of pending rows for relay.

        Rows scheduled for a future retry (``available_at`` in the future) are
        invisible to claims; rows with a ``NULL`` ``available_at`` are always
        eligible.
        """
        ...  # pragma: no cover

    def mark_published(self, ids: Sequence[UUID]) -> Awaitable[int]:
        """Mark claimed rows as published; returns rows updated."""
        ...  # pragma: no cover

    def mark_failed(
        self,
        ids: Sequence[UUID],
        *,
        error: str | None = None,
    ) -> Awaitable[int]:
        """Mark rows as terminally failed; returns rows updated.

        Failed rows stay parked until an operator calls :meth:`requeue_failed`.
        For transient publish errors prefer :meth:`mark_retry`.
        """
        ...  # pragma: no cover

    def mark_retry(
        self,
        ids: Sequence[UUID],
        *,
        attempts: int,
        available_at: datetime,
        error: str | None = None,
    ) -> Awaitable[int]:
        """Reschedule claimed rows for a future retry; returns rows updated.

        Moves ``processing`` rows back to ``pending`` with the durable retry
        counter set to *attempts* and the row invisible to
        :meth:`claim_pending` until *available_at*.
        """
        ...  # pragma: no cover

    def reclaim_stale_processing(
        self,
        *,
        older_than: datetime,
    ) -> Awaitable[int]:
        """Reset stuck processing rows to pending; returns rows updated."""
        ...  # pragma: no cover

    def requeue_failed(self, ids: Sequence[UUID]) -> Awaitable[int]:
        """Reset failed rows to pending for manual or automated re-drive.

        Resets the retry counter (``attempts``) to ``0`` and clears any retry
        schedule — operator intent is a fresh start.
        """
        ...  # pragma: no cover
