"""Outbox and integration-event specifications."""

from typing import Any, Literal, Self, final

import attrs

from forze.base.primitives import StrKey
from forze.base.serialization import ModelCodec

from ..base import BaseSpec

# ----------------------- #

OutboxDestinationKind = Literal["queue", "stream", "pubsub"]


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class OutboxDestination:
    """Relay target for staged integration events."""

    kind: OutboxDestinationKind
    """Transport kind: queue, stream, or pubsub."""

    route: StrKey
    """Deps route name of the registered transport spec (``QueueSpec``, ``StreamSpec``, ``PubSubSpec``)."""

    channel: str
    """Logical channel: queue name, stream name, or pubsub topic."""

    # ....................... #

    @classmethod
    def queue(cls, *, route: StrKey, channel: str) -> Self:
        """Target a :class:`~forze.application.contracts.queue.QueueCommandPort`."""

        return cls(kind="queue", route=route, channel=channel)

    @classmethod
    def stream(cls, *, route: StrKey, channel: str) -> Self:
        """Target a :class:`~forze.application.contracts.stream.StreamCommandPort`."""

        return cls(kind="stream", route=route, channel=channel)

    @classmethod
    def pubsub(cls, *, route: StrKey, channel: str) -> Self:
        """Target a :class:`~forze.application.contracts.pubsub.PubSubCommandPort`.

        **Deliberate delivery downgrade.** The outbox guarantees at-least-once
        only up to the broker; pubsub is at-most-once past it. A relay marks a
        row ``published`` after a fire-and-forget publish, so an event with no
        live subscriber at that moment is silently lost. Legitimate for lossy
        broadcast (cache invalidation, presence, live notifications); choose a
        queue or stream destination when consumers must eventually see every
        event.
        """

        return cls(kind="pubsub", route=route, channel=channel)


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class OutboxSpec[M](BaseSpec):
    """Specification binding an outbox route to its integration-event payload codec."""

    codec: ModelCodec[M, Any]
    """Payload record codec for staged integration events."""

    destination: OutboxDestination | None = None
    """Optional default relay target honored by relay workers."""

    # ....................... #

    @property
    def model_type(self) -> type[M]:
        """Payload model type carried by :attr:`codec`."""

        return self.codec.model_type
