from datetime import datetime
from typing import Mapping, Optional, final

from pydantic import Field

from forze.base.primitives import JsonDict
from forze.domain.models import (
    BaseDTO,
    CoreModel,
    CreateDocumentCmd,
    Document,
    ReadDocument,
)

# ----------------------- #


class OutboxEventImmutableFields(CoreModel):
    """Immutable fields shared across outbox event models."""

    topic: str
    """Target topic for the outbox event."""

    payload: JsonDict
    """Serialized event payload."""

    key: Optional[str] = None
    """Optional partitioning key."""

    headers: Mapping[str, str] = Field(default_factory=dict)
    """Arbitrary string headers attached to the event."""


# ....................... #


class OutboxEventMutableFields(CoreModel):
    """Mutable fields shared across outbox event models."""

    published_at: Optional[datetime] = None
    """Timestamp when the event was published; ``None`` while unpublished."""


# ....................... #


@final
class OutboxEvent(
    Document,
    OutboxEventImmutableFields,
    OutboxEventMutableFields,
):
    """Outbox event model."""


# ....................... #


@final
class CreateOutboxEventCmd(
    CreateDocumentCmd,
    OutboxEventImmutableFields,
):
    """Create outbox event command DTO."""


# ....................... #


@final
class UpdateOutboxEventCmd(
    BaseDTO,
    OutboxEventMutableFields,
):
    """Update outbox event command DTO."""


# ....................... #


@final
class ReadOutboxEvent(
    ReadDocument,
    OutboxEventImmutableFields,
    OutboxEventMutableFields,
):
    """Read outbox event model."""
