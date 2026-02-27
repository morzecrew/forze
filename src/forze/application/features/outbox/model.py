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
    topic: str
    payload: JsonDict
    key: Optional[str] = None
    headers: Mapping[str, str] = Field(default_factory=dict)


# ....................... #


class OutboxEventMutableFields(CoreModel):
    published_at: Optional[datetime] = None


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
