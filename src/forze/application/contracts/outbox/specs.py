"""Outbox and integration-event specifications."""

from typing import Any, Literal, Self, final

import attrs

from forze.base.primitives import StrKey
from forze.base.serialization import ModelCodec

from ..base import BaseSpec, EncryptionReach

# ----------------------- #

OutboxDestinationKind = Literal["queue", "stream", "pubsub"]

OutboxEncryptionTier = EncryptionReach
"""Deprecated alias for :data:`~forze.application.contracts.base.EncryptionReach`.

The outbox encryption setting is a *reach* ladder (where the payload is decrypted), not a
*coverage* tier (how much is encrypted) — an encrypted outbox payload is always a
whole-payload envelope. Kept for back-compat; prefer ``EncryptionReach``."""


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

    encryption: EncryptionReach = "none"
    """Whole-payload encryption *reach* for this route (default ``"none"``).

    ``at_rest`` encrypts the payload in the outbox store and decrypts it in the relay;
    ``end_to_end`` keeps it encrypted through the broker to the consumer. Both require a
    keyring (``CryptoDepsModule``) wired wherever decryption happens — the relay for
    ``at_rest``, the consumer for ``end_to_end``. See :data:`EncryptionReach`."""

    # ....................... #

    @property
    def model_type(self) -> type[M]:
        """Payload model type carried by :attr:`codec`."""

        return self.codec.model_type

    # ....................... #

    @property
    def encrypts(self) -> bool:
        """Whether payloads are encrypted at staging (``at_rest`` or ``end_to_end``)."""

        return self.encryption != "none"

    # ....................... #

    @property
    def relay_decrypts(self) -> bool:
        """Whether the relay decrypts before publish (``at_rest`` only)."""

        return self.encryption == "at_rest"
