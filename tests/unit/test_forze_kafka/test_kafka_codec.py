"""Kafka record codec: value round-trip, encrypted passthrough, header/type split."""

from forze.application.contracts.crypto import ENCRYPTED_PAYLOAD_KEY

from _kafka_fakes import Msg, make_codec

# ----------------------- #


def test_value_roundtrip() -> None:
    codec = make_codec()
    raw = codec.encode_value(Msg(body="hello"))

    assert isinstance(raw, bytes)
    assert codec.decode_value(raw) == Msg(body="hello")


def test_encrypted_payload_passes_through() -> None:
    codec = make_codec()
    wrapper = {ENCRYPTED_PAYLOAD_KEY: "Zm9vYmFy"}  # a one-key ciphertext envelope

    raw = codec.encode_value(wrapper)  # type: ignore[arg-type]

    # The wrapper survives opaquely — the consumer (runner) decrypts it, the
    # codec must not try to decode it as the payload model.
    assert codec.decode_value(raw) == wrapper


def test_encode_headers_splits_type_out() -> None:
    codec = make_codec()

    encoded = dict(
        codec.encode_headers(
            type="order.created",
            headers={"forze_event_id": "e1", "traceparent": "tp"},
        )
    )

    assert encoded["forze_type"] == b"order.created"
    assert encoded["forze_event_id"] == b"e1"
    assert encoded["traceparent"] == b"tp"


def test_reserved_type_header_wins_over_caller_key() -> None:
    codec = make_codec()

    encoded = dict(codec.encode_headers(type="real", headers={"forze_type": "caller"}))

    assert encoded["forze_type"] == b"real"


def test_decode_headers_lifts_type() -> None:
    codec = make_codec()

    headers, message_type = codec.decode_headers(
        [("forze_type", b"evt"), ("forze_event_id", b"e1")]
    )

    assert message_type == "evt"
    assert headers == {"forze_event_id": "e1"}
    assert "forze_type" not in headers


def test_decode_headers_none() -> None:
    codec = make_codec()

    headers, message_type = codec.decode_headers(None)

    assert headers == {}
    assert message_type is None
