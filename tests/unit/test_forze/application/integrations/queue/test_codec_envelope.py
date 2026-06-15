"""QueueMessageCodec passes encrypted-envelope payloads through (e2e transport hop)."""

from __future__ import annotations

import orjson
from pydantic import BaseModel

from forze.application.contracts.crypto import (
    is_encrypted_payload,
    wrap_encrypted_payload,
)
from forze.application.integrations.queue.codec import (
    BaseQueueMessage,
    QueueMessageCodec,
)
from forze.base.serialization import PydanticModelCodec

# ----------------------- #


class _Payload(BaseModel):
    n: int


def _codec() -> QueueMessageCodec[_Payload]:
    return QueueMessageCodec(payload_codec=PydanticModelCodec(_Payload))


def _raw(body: bytes) -> BaseQueueMessage:
    return BaseQueueMessage(queue="q", id="1", body=body)


# ....................... #


def test_plaintext_round_trip_unchanged() -> None:
    codec = _codec()

    body = codec.encode(_Payload(n=7))
    message = codec.decode("q", _raw(body))

    assert message.payload == _Payload(n=7)


def test_encode_passes_envelope_wrapper_through() -> None:
    codec = _codec()
    wrapper = wrap_encrypted_payload("Y2lwaGVy")  # base64 "cipher"

    body = codec.encode(wrapper)  # type: ignore[arg-type]

    assert orjson.loads(body) == wrapper


def test_decode_passes_envelope_body_through_undecoded() -> None:
    codec = _codec()
    wrapper = wrap_encrypted_payload("Y2lwaGVy")
    body = orjson.dumps(wrapper)

    message = codec.decode("q", _raw(body))

    # The wrapper survives to the consumer runner (which decrypts it), not decoded to _Payload.
    assert is_encrypted_payload(message.payload)
    assert message.payload == wrapper


def test_encrypted_round_trip_through_codec() -> None:
    codec = _codec()
    wrapper = wrap_encrypted_payload("Y2lwaGVy")

    body = codec.encode(wrapper)  # type: ignore[arg-type]
    message = codec.decode("q", _raw(body))

    assert message.payload == wrapper
