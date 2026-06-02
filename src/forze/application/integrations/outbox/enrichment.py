"""Outbox event enrichment protocol."""

from __future__ import annotations

from datetime import datetime
from typing import Protocol, runtime_checkable
from uuid import UUID

from pydantic import BaseModel

from forze.application.contracts.outbox import IntegrationEvent

# ----------------------- #


@runtime_checkable
class OutboxEventEnricher(Protocol):
    """Build :class:`~forze.application.contracts.outbox.IntegrationEvent` with envelope fields."""

    def enrich[M: BaseModel](
        self,
        event_type: str,
        payload: M,
        *,
        event_id: UUID | None = None,
        occurred_at: datetime | None = None,
    ) -> IntegrationEvent[M]:
        """Return a fully enriched integration event."""
        ...
