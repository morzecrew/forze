"""Shared queue payload codec for message-queue adapters (SQS, RabbitMQ, ...)."""

from datetime import datetime
from typing import Any, Mapping, Protocol

import attrs
import orjson

from forze.application.contracts.crypto import (
    is_encrypted_payload,
    looks_encrypted_body,
)
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

    @property
    def headers(self) -> Mapping[str, str] | None: ...

    @property
    def delivery_count(self) -> int | None: ...


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
    headers: Mapping[str, str] | None = None
    """Caller-visible transport headers (reserved transport keys excluded)."""

    delivery_count: int | None = None
    """Approximate deliveries including this one; ``None`` when unknown."""


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
        # End-to-end encrypted payloads arrive here as the one-key envelope wrapper
        # (the relay forwards ciphertext). Serialize it opaquely — the model codec
        # would reject a wrapper that is not its own model.
        if is_encrypted_payload(payload):
            return orjson.dumps(payload)

        return self.payload_codec.encode_json_bytes(payload)

    # ....................... #

    def decode(self, queue: str, raw: RawQueueMessage) -> QueueMessage[M]:
        # An encrypted-envelope body is passed through as the wrapper (the consumer
        # runner decrypts it before the handler); plaintext decodes to the model as
        # before, byte-for-byte — the peek avoids parsing normal bodies twice.
        payload: Any
        if looks_encrypted_body(raw.body):
            # The peek matches a serialized prefix only; confirm it is a genuine
            # one-key wrapper before diverting. A plaintext body that merely shares
            # the prefix falls back to the model codec (double-parse on rare collision).
            candidate = orjson.loads(raw.body)
            payload = (
                candidate
                if is_encrypted_payload(candidate)
                else self.payload_codec.decode_json_bytes(raw.body)
            )
        else:
            payload = self.payload_codec.decode_json_bytes(raw.body)

        return QueueMessage(
            queue=queue,
            id=raw.id,
            payload=payload,
            type=raw.type,
            enqueued_at=raw.enqueued_at,
            key=raw.key,
            headers=raw.headers or {},
            delivery_count=raw.delivery_count,
        )
