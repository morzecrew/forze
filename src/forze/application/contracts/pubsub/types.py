from datetime import datetime
from typing import NotRequired, TypedDict

from pydantic import BaseModel

# ----------------------- #


class PubSubMessage[M: BaseModel](TypedDict):
    """Message as read from or written to a pubsub backend."""

    topic: str
    """Logical topic or channel."""

    payload: M
    """Structured payload carried by the message."""

    type: NotRequired[str | None]
    """Optional message type or category."""

    published_at: NotRequired[datetime | None]
    """Optional timestamp associated with the message."""

    key: NotRequired[str | None]
    """Optional partitioning key for the message."""
