"""Unit tests for :mod:`forze_redis.kernel.platform.utils`."""

from forze_redis.kernel.platform.utils import parse_pubsub_message, parse_stream_entries


class TestParseStreamEntries:
    """Tests for :func:`parse_stream_entries`."""

    def test_none_and_empty_return_empty_list(self) -> None:
        assert parse_stream_entries(None) == []
        assert parse_stream_entries([]) == []

    def test_decodes_stream_and_message_ids(self) -> None:
        raw = [
            (
                b"stream-a",
                [
                    (
                        b"1-0",
                        {b"field": b"value"},
                    )
                ],
            )
        ]
        out = parse_stream_entries(raw)
        assert out == [("stream-a", [("1-0", {b"field": b"value"})])]

    def test_non_bytes_stream_name_coerced(self) -> None:
        raw = [(123, [(b"1-0", {b"k": b"v"})])]
        out = parse_stream_entries(raw)
        assert out[0][0] == "123"

    def test_list_of_pairs_field_data(self) -> None:
        """Field data as tuple pairs (non-dict) is normalized."""
        raw = [
            (
                "s",
                [
                    (
                        "1-1",
                        [(b"a", 1), (b"b", "x")],
                    )
                ],
            )
        ]
        out = parse_stream_entries(raw)
        _, messages = out[0]
        fields = messages[0][1]
        assert fields[b"a"] == b"1"
        assert fields[b"b"] == b"x"


class TestParsePubsubMessage:
    """Tests for :func:`parse_pubsub_message`."""

    def test_returns_none_for_wrong_type(self) -> None:
        assert parse_pubsub_message({"type": "subscribe"}) is None
        assert parse_pubsub_message({"type": "pmessage"}) is None

    def test_returns_none_when_channel_or_data_missing(self) -> None:
        assert parse_pubsub_message({"type": "message"}) is None
        assert parse_pubsub_message({"type": "message", "channel": b"c"}) is None

    def test_message_with_bytes_channel_and_data(self) -> None:
        raw = {"type": b"message", "channel": b"chan", "data": b"payload"}
        out = parse_pubsub_message(raw)
        assert out is not None
        channel, data = out
        assert channel == "chan"
        assert data == b"payload"

    def test_string_channel_and_non_bytes_data(self) -> None:
        raw = {"type": "message", "channel": "c", "data": 99}
        out = parse_pubsub_message(raw)
        assert out is not None
        assert out[0] == "c"
        assert out[1] == b"99"
