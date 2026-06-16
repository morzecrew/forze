from enum import StrEnum
from functools import cached_property
from typing import Any, Literal

import attrs

from forze.base.primitives import StrKeyNamespace
from forze.base.serialization import ModelCodec

# ----------------------- #

MessageEncryptionTier = Literal["none", "end_to_end"]
"""Whole-payload encryption for a direct-messaging route. ``none`` publishes plaintext;
``end_to_end`` seals the payload through the broker so the consumer decrypts it (there is
no ``at_rest`` tier — a transport has no store of its own; the outbox owns at-rest)."""


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
    """Whole-payload encryption tier for this route (default ``none``). When
    ``end_to_end``, published payloads are sealed and the consumer decrypts them."""

    # ....................... #

    @property
    def model_type(self) -> type[M]:
        """Payload model type carried by :attr:`codec`."""

        return self.codec.model_type

    @property
    def encrypts(self) -> bool:
        """Whether this route seals its payloads (encryption tier above ``none``)."""

        return self.encryption != "none"
