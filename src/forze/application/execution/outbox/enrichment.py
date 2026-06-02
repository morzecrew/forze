"""Integration-event enrichment from invocation context."""

from __future__ import annotations

from datetime import datetime
from typing import final
from uuid import UUID

import attrs
from pydantic import BaseModel

from forze.application.contracts.outbox import IntegrationEvent
from forze.application.execution.context.invocation import InvocationContext
from forze.base.primitives import utcnow, uuid7

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class InvocationOutboxEnricher:
    """Fill tenant and invocation metadata from :class:`InvocationContext`."""

    inv: InvocationContext
    """Active invocation context for the request."""

    def enrich[M: BaseModel](
        self,
        event_type: str,
        payload: M,
        *,
        event_id: UUID | None = None,
        occurred_at: datetime | None = None,
    ) -> IntegrationEvent[M]:
        event_id = event_id or uuid7()
        metadata = self.inv.get_metadata()
        tenant = self.inv.get_tenant()

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
