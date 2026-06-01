"""Outbox command and query port protocols."""

from datetime import datetime
from typing import Any, Awaitable, Protocol, Sequence, runtime_checkable
from uuid import UUID

from pydantic import BaseModel

from .specs import OutboxSpec
from .value_objects import IntegrationEvent, OutboxClaim

# ----------------------- #


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
    ) -> Awaitable[None]:
        """Buffer an integration event for a later :meth:`flush`."""
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
    """Claim and update staged outbox rows for relay workers."""

    spec: OutboxSpec[Any]
    """Outbox specification for this port instance."""

    def claim_pending(
        self,
        *,
        limit: int | None = None,
    ) -> Awaitable[Sequence[OutboxClaim]]:
        """Claim a batch of pending rows for relay."""
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
        """Mark rows as failed; returns rows updated."""
        ...  # pragma: no cover

    def reclaim_stale_processing(
        self,
        *,
        older_than: datetime,
    ) -> Awaitable[int]:
        """Reset stuck processing rows to pending; returns rows updated."""
        ...  # pragma: no cover
