from datetime import datetime
from typing import final

import attrs

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PubSubMessage[M]:
    """Message as read from or written to a pubsub backend."""

    topic: str
    """Logical topic or channel."""

    payload: M
    """Structured payload carried by the message."""

    type: str | None = None
    """Optional message type or category."""

    published_at: datetime | None = None
    """Optional timestamp associated with the message."""

    key: str | None = None
    """Optional partitioning key for the message."""
