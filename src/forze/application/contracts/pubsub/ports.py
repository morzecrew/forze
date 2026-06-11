from datetime import datetime, timedelta
from typing import (
    AsyncGenerator,
    Awaitable,
    Mapping,
    Protocol,
    Sequence,
    runtime_checkable,
)

from .value_objects import PubSubMessage

# ----------------------- #


@runtime_checkable
class PubSubCommandPort[M](Protocol):
    """Contract for publishing messages to a pubsub backend."""

    def publish(
        self,
        topic: str,
        payload: M,
        *,
        type: str | None = None,
        key: str | None = None,
        published_at: datetime | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> Awaitable[None]:
        """Publish a single message to *topic*.

        :param headers: String-to-string transport metadata, propagated
            best-effort via the backend's native metadata channel and surfaced
            on received messages as ``PubSubMessage.headers``. Not part of the
            payload contract.
        """
        ...


# ....................... #


@runtime_checkable
class PubSubQueryPort[M](Protocol):
    """Contract for subscribing to messages from a pubsub backend."""

    def subscribe(
        self,
        topics: Sequence[str],
        *,
        timeout: timedelta | None = None,
    ) -> AsyncGenerator[PubSubMessage[M]]:
        """Yield messages from the given *topics* until *timeout* elapses."""
        ...
