"""Value objects for transactional outbox staging and relay."""

from datetime import datetime
from enum import StrEnum
from typing import Any, final
from uuid import UUID

import attrs

from forze.base.primitives import JsonDict, utcnow

# ----------------------- #


class OutboxStatus(StrEnum):
    """Lifecycle status of a staged outbox row."""

    PENDING = "pending"
    PROCESSING = "processing"
    PUBLISHED = "published"
    FAILED = "failed"


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class IntegrationEvent[M]:
    """Integration event staged for outbox persistence and relay.

    Envelope fields (``tenant_id``, correlation, execution ids) are typically
    filled by the staging coordinator from :class:`~forze.application.execution.context.ExecutionContext`.
    """

    event_type: str
    """Logical event name (for example ``project.created``)."""

    payload: M
    """Typed payload serialized via :class:`~.OutboxSpec.codec`."""

    event_id: UUID
    """Idempotency key for staging and relay deduplication."""

    occurred_at: datetime = attrs.field(factory=utcnow)
    """When the event occurred (UTC)."""

    tenant_id: UUID | None = None
    """Optional tenant scope copied from invocation context."""

    execution_id: UUID | None = None
    """Optional execution id from invocation metadata."""

    correlation_id: UUID | None = None
    """Optional correlation id from invocation metadata."""

    causation_id: UUID | None = None
    """Optional causation id from invocation metadata."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class StagedOutboxEntry:
    """Internal row materialized from a staged integration event before flush."""

    outbox_route: str
    """Logical outbox route (:attr:`~.OutboxSpec.name`)."""

    event: IntegrationEvent[Any]
    """Staged integration event."""

    payload_json: JsonDict
    """Serialized payload for persistence."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class OutboxClaim:
    """A pending outbox row claimed for relay."""

    id: UUID
    """Primary key of the outbox row."""

    outbox_route: str
    """Logical outbox route."""

    event_id: UUID
    """Staged event idempotency key."""

    event_type: str
    """Logical event name."""

    payload: JsonDict
    """Decoded JSON payload for relay."""

    tenant_id: UUID | None = None
    """Optional tenant scope."""

    execution_id: UUID | None = None
    """Optional execution id."""

    correlation_id: UUID | None = None
    """Optional correlation id."""

    causation_id: UUID | None = None
    """Optional causation id."""

    occurred_at: datetime | None = None
    """When the event occurred."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class OutboxRelayResult:
    """Summary of a single relay pass."""

    claimed: int = 0
    """Rows claimed from the outbox."""

    published: int = 0
    """Rows successfully enqueued and marked published."""

    failed: int = 0
    """Rows marked failed during relay."""

    reclaimed: int = 0
    """Rows reset from processing to pending before claim."""
