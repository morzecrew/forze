"""Domain event base."""

from datetime import datetime
from uuid import UUID

from pydantic import Field

from forze.base.primitives import utcnow, uuid7

from .base import BaseDTO

# ----------------------- #


class DomainEvent(BaseDTO):
    """Base class for domain events raised by aggregates.

    A frozen value object recording something that happened in the domain.
    Subclasses add the aggregate identifier and payload fields. Domain events are
    dispatched in-process within the operation's transaction; a bridge handler may
    translate one into a transactional-outbox integration event.
    """

    event_id: UUID = Field(default_factory=uuid7, frozen=True)
    """Unique identifier of the event."""

    occurred_at: datetime = Field(default_factory=utcnow, frozen=True)
    """Timestamp when the event occurred."""
