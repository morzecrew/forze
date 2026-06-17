"""Value objects for transactional outbox staging and relay."""

from datetime import datetime
from enum import StrEnum
from typing import Any, final
from uuid import UUID

import attrs

from forze.base.primitives import HlcTimestamp, JsonDict, StrKey, utcnow

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

    event_type: StrKey
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

    ordering_key: str | None = None
    """Optional partition key for delivery ordering (typically the aggregate id).

    On capable transports the relay publishes it as the message ``key``
    (SQS FIFO ``MessageGroupId``, stream partition key), so same-key events
    relay in ``created_at`` order on the happy path. Best-effort by design:
    a retrying or failed row does **not** stall later rows of its key —
    consumers must still tolerate reordering and redelivery.
    """

    hlc: HlcTimestamp | None = None
    """Hybrid Logical Clock stamp (causal order). Stamped from the process-global
    :func:`~forze.application.execution.outbox.clock.outbox_clock` at staging; a
    causal successor always sorts after its cause across replicas. Persisted and
    used for claim ordering only on outbox backends with HLC ordering enabled;
    otherwise carried for downstream consumers that order on ``HEADER_HLC``."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class StagedOutboxEntry:
    """Internal row materialized from a staged integration event before flush."""

    outbox_route: StrKey
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

    outbox_route: StrKey
    """Logical outbox route."""

    event_id: UUID
    """Staged event idempotency key."""

    event_type: StrKey
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

    attempts: int = 0
    """Completed publish attempts so far (durable retry counter)."""

    ordering_key: str | None = None
    """Optional partition key staged with the event (see :class:`IntegrationEvent`).

    The relay publishes ``ordering_key or str(event_id)`` as the transport
    ``key``: same-key events partition together on capable transports (SQS
    FIFO ``MessageGroupId``, stream partition key) and relay in ``created_at``
    order on the happy path. A retrying/failed row does **not** stall later
    rows of its key.
    """

    hlc: HlcTimestamp | None = None
    """Hybrid Logical Clock stamp reconstructed from the row (HLC-ordering
    backends only). The relay forwards it as ``HEADER_HLC`` so consumers can
    order causally; ``None`` when the backend does not persist it."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class OutboxRelayResult:
    """Summary of a single relay pass.

    Delivery is at-least-once and ordering is **not** preserved across
    failures/retries — consumers must key on ``event_id`` and tolerate
    reordering as well as redelivery.
    """

    claimed: int = 0
    """Rows claimed from the outbox."""

    published: int = 0
    """Rows successfully enqueued and marked published."""

    failed: int = 0
    """Rows marked terminally failed during relay."""

    retried: int = 0
    """Rows rescheduled for a future retry after a transient publish error."""

    reclaimed: int = 0
    """Rows reset from processing to pending before claim."""
