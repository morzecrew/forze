"""Shared queue payload codec for message-queue adapters (SQS, RabbitMQ, ...)."""

from datetime import datetime
from typing import Any, Protocol

import attrs

from forze.application.contracts.queue import QueueMessage
from forze.base.serialization import ModelCodec

# ----------------------- #


class RawQueueMessage(Protocol):
    """Structural shape of a backend-received queue message."""

    @property
    def id(self) -> str: ...

    @property
    def body(self) -> bytes: ...

    @property
    def type(self) -> str | None: ...

    @property
    def enqueued_at(self) -> datetime | None: ...

    @property
    def key(self) -> str | None: ...


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class BaseQueueMessage:
    """Backend-agnostic raw queue message envelope.

    Concrete backend message types subclass this; the field shape conforms to
    :class:`RawQueueMessage`.
    """

    queue: str
    id: str
    body: bytes
    type: str | None = None
    enqueued_at: datetime | None = None
    key: str | None = None


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class QueueMessageCodec[M]:
    """Queue payload codec backed by a record-mapping :class:`ModelCodec`.

    Backends subclass this to bind their concrete raw-message type; the
    encode/decode envelope is identical across queue integrations.
    """

    payload_codec: ModelCodec[M, Any]
    """Codec for queue message payloads."""

    # ....................... #

    def encode(self, payload: M) -> bytes:
        return self.payload_codec.encode_json_bytes(payload)

    # ....................... #

    def decode(self, queue: str, raw: RawQueueMessage) -> QueueMessage[M]:
        return QueueMessage(
            queue=queue,
            id=raw.id,
            payload=self.payload_codec.decode_json_bytes(raw.body),
            type=raw.type,
            enqueued_at=raw.enqueued_at,
            key=raw.key,
        )
