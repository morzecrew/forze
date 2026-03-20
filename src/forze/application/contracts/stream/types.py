from datetime import datetime
from typing import NotRequired, TypedDict

from pydantic import BaseModel

# ----------------------- #


class StreamMessage[M: BaseModel](TypedDict):
    """Message as read from or written to a stream backend."""

    stream: str
    """Logical stream name or topic."""

    id: str
    """Backend-specific identifier for the message."""

    payload: M
    """Structured payload carried by the message."""

    type: NotRequired[str | None]
    """Optional message type or category."""

    timestamp: NotRequired[datetime | None]
    """Optional timestamp associated with the message."""

    key: NotRequired[str | None]
    """Optional partitioning key for the message."""
