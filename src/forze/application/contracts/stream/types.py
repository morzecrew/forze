from datetime import datetime
from typing import NotRequired, Optional, TypedDict

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

    type: NotRequired[Optional[str]]
    """Optional message type or category."""

    timestamp: NotRequired[Optional[datetime]]
    """Optional timestamp associated with the message."""

    key: NotRequired[Optional[str]]
    """Optional partitioning key for the message."""
