from datetime import datetime, timedelta
from typing import (
    AsyncIterator,
    Awaitable,
    Protocol,
    Sequence,
    runtime_checkable,
)

from pydantic import BaseModel

from .types import PubSubMessage

# ----------------------- #


@runtime_checkable
class PubSubPublishPort[M: BaseModel](Protocol):
    """Contract for publishing messages to a pubsub backend."""

    def publish(
        self,
        topic: str,
        payload: M,
        *,
        type: str | None = None,
        key: str | None = None,
        published_at: datetime | None = None,
    ) -> Awaitable[None]:
        """Publish a single message to *topic*."""
        ...


# ....................... #


@runtime_checkable
class PubSubSubscribePort[M: BaseModel](Protocol):
    """Contract for subscribing to messages from a pubsub backend."""

    def subscribe(
        self,
        topics: Sequence[str],
        *,
        timeout: timedelta | None = None,
    ) -> AsyncIterator[PubSubMessage[M]]:
        """Yield messages from the given *topics* until *timeout* elapses."""
        ...
