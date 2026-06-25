from enum import StrEnum
from functools import cached_property
from typing import Any, Literal

import attrs

from forze.base.primitives import StrKeyNamespace
from forze.base.serialization import ModelCodec

# ----------------------- #

EncryptionReach = Literal["none", "at_rest", "end_to_end"]
"""Where a whole-payload envelope is decrypted — the messaging *reach* ladder.

Reach answers "who holds the key and unwraps the payload", not "how much is encrypted"
(an encrypted message is always a whole-payload envelope; field-level coverage is a
storage concern, see :data:`~forze.application.contracts.crypto.EncryptionTier`). Weakest →
strongest:

- ``none`` — no envelope; the payload is stored/published in plaintext.
- ``at_rest`` — encrypted at staging and decrypted by the **relay** before publish, so the
  broker and consumer see plaintext. Protects a store against compromise; applies only
  where a store exists (the outbox), since the producer and relay share a keyring.
- ``end_to_end`` — encrypted at staging and decrypted by the **consumer**, sealed through
  the broker the whole way. The consumer service must have a keyring wired.
"""

# ....................... #

MessageEncryptionTier = Literal["none", "end_to_end"]
"""The direct-transport subset of :data:`EncryptionReach` (no ``at_rest``).

A queue/stream/pub-sub route has no store of its own, so the relay-decrypt level is
inapplicable — only ``none`` (plaintext) or ``end_to_end`` (sealed through the broker,
consumer decrypts). The outbox owns the ``at_rest`` level."""


@attrs.define(slots=True, kw_only=True, frozen=True)
class BaseSpec:
    """Base resource specification."""

    name: str | StrEnum
    """Logical name for the resource."""

    # ....................... #

    @cached_property
    def default_namespace(self) -> StrKeyNamespace:
        """Default namespace for the resource."""

        return StrKeyNamespace(prefix=self.name)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class MessageCodecSpec[M](BaseSpec):
    """Base specification binding a messaging namespace to its payload record codec.

    Shared by queue, pubsub, and stream specs; each only narrows the docstring.
    """

    codec: ModelCodec[M, Any]
    """Payload record codec for messages in this namespace."""

    encryption: MessageEncryptionTier = "none"
    """Whole-payload encryption reach for this route (default ``none``). When
    ``end_to_end``, published payloads are sealed and the consumer decrypts them."""

    # ....................... #

    @property
    def model_type(self) -> type[M]:
        """Payload model type carried by :attr:`codec`."""

        return self.codec.model_type

    @property
    def encrypts(self) -> bool:
        """Whether this route seals its payloads (encryption reach above ``none``)."""

        return self.encryption != "none"
