from collections.abc import AsyncGenerator, Awaitable, Mapping, Sequence
from datetime import datetime, timedelta
from typing import (
    Protocol,
    runtime_checkable,
)

from .value_objects import PubSubMessage

# ----------------------- #


@runtime_checkable
class PubSubCommandPort[M](Protocol):
    """Contract for publishing messages to a pubsub backend.

    Delivery is **at-most-once** (fire-and-forget): a publish succeeds once
    the backend accepts the message, regardless of how many subscribers —
    zero included — receive it. Nothing is persisted or redelivered, so a
    message published while a subscriber is down (or before it subscribes)
    is silently lost. Use a queue or stream port for must-arrive messaging.
    """

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

        Fire-and-forget: returning successfully means the backend accepted
        the message, **not** that any subscriber received it (at-most-once;
        see the port docstring).

        :param headers: String-to-string transport metadata, propagated
            best-effort via the backend's native metadata channel and surfaced
            on received messages as ``PubSubMessage.headers``. Not part of the
            payload contract.
        """
        ...


# ....................... #


@runtime_checkable
class PubSubQueryPort[M](Protocol):
    """Contract for subscribing to messages from a pubsub backend.

    Subscription is live-only: only messages published while the
    subscription is active are delivered — there is no history, replay, or
    redelivery after a failure (at-most-once; see
    :class:`PubSubCommandPort`).
    """

    def subscribe(
        self,
        topics: Sequence[str],
        *,
        timeout: timedelta | None = None,
    ) -> AsyncGenerator[PubSubMessage[M]]:
        """Yield messages from the given *topics* until *timeout* elapses."""
        ...
