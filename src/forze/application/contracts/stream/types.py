from typing import NotRequired, Optional, TypedDict

from pydantic import BaseModel

# ----------------------- #


class StreamEvent[M: BaseModel](TypedDict):
    """Event as read from or written to a stream backend.

    Backend-specific identifiers (e.g. Redis stream ID) are in ``id``.
    """

    stream: str
    """Logical stream name or topic."""

    id: str
    """Backend-specific identifier for the event (e.g. Redis stream ID)."""

    type: NotRequired[Optional[str]]
    """Optional event type or category."""

    timestamp: NotRequired[Optional[int]]
    """Optional timestamp associated with the event."""

    key: NotRequired[Optional[str]]
    """Optional partitioning key for the event."""

    data: M
    """Structured payload carried by the event."""
