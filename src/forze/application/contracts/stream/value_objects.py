from datetime import datetime
from typing import final

import attrs

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class StreamMessage[M]:
    """Message as read from or written to a stream backend."""

    stream: str
    """Logical stream name or topic."""

    id: str
    """Backend-specific identifier for the message."""

    payload: M
    """Structured payload carried by the message."""

    type: str | None = None
    """Optional message type or category."""

    timestamp: datetime | None = None
    """Optional timestamp associated with the message."""

    key: str | None = None
    """Optional partitioning key for the message."""
