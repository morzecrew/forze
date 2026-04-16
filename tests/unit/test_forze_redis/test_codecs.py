"""Unit tests for Redis stream / pubsub codecs (edge cases)."""

from datetime import datetime

import pytest
from pydantic import BaseModel

from forze.base.errors import CoreError
from forze_redis.adapters.codecs import RedisPubSubCodec, RedisStreamCodec


class _M(BaseModel):
    v: int


def test_pubsub_codec_decode_non_object_json_raises() -> None:
    codec = RedisPubSubCodec(model=_M)

    with pytest.raises(CoreError, match="invalid payload"):
        codec.decode("t", b"[1,2,3]")


def test_pubsub_codec_decode_payload_not_string_raises() -> None:
    codec = RedisPubSubCodec(model=_M)

    with pytest.raises(CoreError, match="no payload"):
        codec.decode("t", b'{"payload": 42}')


def test_stream_codec_encode_with_optional_fields() -> None:
    codec = RedisStreamCodec(model=_M)
    ts = datetime(2026, 4, 15, 12, 0, 0)
    fields = codec.encode(_M(v=7), type="evt", key="k1", timestamp=ts)

    assert fields["payload"] == '{"v":7}'
    assert fields["type"] == "evt"
    assert fields["key"] == "k1"
    assert fields["timestamp"] == ts.isoformat()


def test_stream_codec_decode_roundtrip_with_timestamp() -> None:
    codec = RedisStreamCodec(model=_M)
    ts = datetime(2026, 3, 1, 8, 30, 0)
    raw = {
        b"payload": b'{"v":3}',
        b"type": b"created",
        b"key": b"pk",
        b"timestamp": ts.isoformat().encode(),
    }
    msg = codec.decode("s", "0-1", raw)

    assert msg["stream"] == "s"
    assert msg["id"] == "0-1"
    assert msg["payload"].v == 3
    assert msg["type"] == "created"
    assert msg["key"] == "pk"
    assert msg["timestamp"] == ts


def test_stream_codec_decode_missing_payload_raises() -> None:
    codec = RedisStreamCodec(model=_M)

    with pytest.raises(CoreError, match="no payload"):
        codec.decode("s", "0-1", {b"type": b"x"})
