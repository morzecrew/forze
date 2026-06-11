"""Header round-trip through the Redis stream/pubsub JSON envelopes."""

from __future__ import annotations

import pytest
from pydantic import BaseModel

pytest.importorskip("redis")

from forze.base.serialization import PydanticModelCodec
from forze_redis.adapters.codecs import RedisPubSubCodec, RedisStreamCodec

# ----------------------- #


class _Payload(BaseModel):
    value: str


# ----------------------- #


def test_stream_codec_headers_round_trip() -> None:
    codec = RedisStreamCodec(payload_codec=PydanticModelCodec(_Payload))

    fields = codec.encode(
        _Payload(value="hello"),
        type="created",
        key="k-1",
        headers={"forze_correlation_id": "abc", "trace": "t-1"},
    )

    assert "headers" in fields

    decoded = codec.decode(
        "audit",
        "1-1",
        {k.encode(): v.encode() for k, v in fields.items()},
    )

    assert decoded.headers == {"forze_correlation_id": "abc", "trace": "t-1"}
    assert decoded.type == "created"
    assert decoded.key == "k-1"
    assert decoded.payload.value == "hello"


def test_stream_codec_without_headers_omits_field_and_decodes_empty() -> None:
    codec = RedisStreamCodec(payload_codec=PydanticModelCodec(_Payload))

    fields = codec.encode(_Payload(value="hello"))

    assert "headers" not in fields

    decoded = codec.decode(
        "audit",
        "1-1",
        {k.encode(): v.encode() for k, v in fields.items()},
    )

    assert dict(decoded.headers) == {}


def test_stream_codec_tolerates_malformed_headers_field() -> None:
    codec = RedisStreamCodec(payload_codec=PydanticModelCodec(_Payload))
    fields = codec.encode(_Payload(value="hello"))
    fields["headers"] = "not-json"

    decoded = codec.decode(
        "audit",
        "1-1",
        {k.encode(): v.encode() for k, v in fields.items()},
    )

    assert dict(decoded.headers) == {}


# ....................... #


def test_pubsub_codec_headers_round_trip() -> None:
    codec = RedisPubSubCodec(payload_codec=PydanticModelCodec(_Payload))

    raw = codec.encode(
        _Payload(value="hello"),
        type="created",
        key="k-1",
        headers={"forze_event_id": "evt", "trace": "t-1"},
    )
    decoded = codec.decode("events", raw)

    assert decoded.headers == {"forze_event_id": "evt", "trace": "t-1"}
    assert decoded.type == "created"
    assert decoded.key == "k-1"
    assert decoded.payload.value == "hello"


def test_pubsub_codec_without_headers_decodes_empty() -> None:
    codec = RedisPubSubCodec(payload_codec=PydanticModelCodec(_Payload))

    raw = codec.encode(_Payload(value="hello"))
    decoded = codec.decode("events", raw)

    assert dict(decoded.headers) == {}
