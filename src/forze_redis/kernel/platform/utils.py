"""Parsing utilities that normalise raw ``redis-py`` responses into typed structures."""

from collections.abc import Iterable
from typing import Any, cast

from redis.typing import (
    StreamEntry,
    StreamRangeResponse,
    XReadGroupResponse,
    XReadGroupStreamResponse,
    XReadResponse,
)

from .types import (
    RawRedisPubSubMessage,
    RawRedisStreamResponse,
    RedisPubSubMessage,
    RedisStreamEntry,
    RedisStreamFields,
    RedisStreamResponse,
)

# ----------------------- #


def parse_stream_entries(raw: RawRedisStreamResponse) -> RedisStreamResponse:
    """Normalise a raw ``XREAD``/``XREADGROUP`` response into :data:`RedisStreamResponse`.

    Decodes byte stream names and message IDs to strings, and coerces field
    keys and values to ``bytes``.  Returns an empty list when *raw* is
    ``None`` or empty.

    Accepts both legacy RESP2 list batches and redis-py v8 unified dict
    responses (``XReadResponse`` / ``XReadGroupResponse``).

    :param raw: Raw response from ``redis-py``.
    :returns: Parsed list of stream batches.
    """

    if raw is None or not raw:
        return []

    return [
        (_to_str(stream), _parse_stream_messages(_flatten_stream_messages(messages)))
        for stream, messages in _iter_stream_batches(raw)
    ]


# ....................... #


def parse_pubsub_message(raw: RawRedisPubSubMessage) -> RedisPubSubMessage | None:
    """Extract channel and payload from a raw pub/sub message dict.

    Only messages whose ``type`` is ``"message"`` are considered valid.
    Returns ``None`` for subscribe/unsubscribe confirmations, missing
    fields, or unrecognised message types.

    :param raw: Raw message dict from ``redis-py``.
    :returns: A ``(channel, data)`` tuple or ``None``.
    """

    msg_type = raw.get("type")

    if msg_type not in {"message", b"message"}:
        return None

    channel_raw = raw.get("channel")
    data_raw = raw.get("data")

    if channel_raw is None or data_raw is None:
        return None

    return _to_str(channel_raw), _to_bytes(data_raw)


# ....................... #
# Internals


def _iter_stream_batches(
    raw: XReadResponse | XReadGroupResponse,
) -> Iterable[tuple[str | bytes, StreamRangeResponse | list[StreamRangeResponse]]]:
    """Yield ``(stream_name, messages)`` from any redis-py XREAD wire shape."""

    if isinstance(raw, dict):
        yield from raw.items()  # type: ignore[misc]
        return

    for stream, messages in raw:
        yield stream, messages


# ....................... #


def _flatten_stream_messages(
    messages: (
        StreamRangeResponse | list[StreamRangeResponse] | XReadGroupStreamResponse
    ),
) -> list[StreamEntry]:
    """Flatten per-stream message containers to a single entry list."""

    if not messages:
        return []

    if isinstance(messages[0], list):
        flattened: list[StreamEntry] = []

        for batch in messages:
            flattened.extend(cast(StreamRangeResponse, batch))  # type: ignore[redundant-cast]

        return flattened

    return cast(StreamRangeResponse, messages)  # type: ignore[redundant-cast]


# ....................... #


def _parse_stream_messages(messages: list[StreamEntry]) -> list[RedisStreamEntry]:
    """Parse a list of raw stream messages into normalized entries."""

    out: list[RedisStreamEntry] = []

    for msg_id_raw, data_raw in messages:
        if msg_id_raw is None or data_raw is None:
            continue

        msg_id = _to_str(msg_id_raw)

        if hasattr(data_raw, "items"):
            data_dict: Iterable[tuple[Any, Any]] = (
                data_raw.items()
            )  # pyright: ignore[reportAttributeAccessIssue]
        else:
            data_dict = cast(Iterable[tuple[Any, Any]], data_raw)

        normalized: RedisStreamFields = {
            _to_bytes(k): _to_bytes(v) for k, v in data_dict
        }

        out.append((msg_id, normalized))

    return out


# ....................... #


def _to_str(value: str | bytes | Any) -> str:
    """Coerce a value to a string, decoding bytes as UTF-8."""

    if isinstance(value, (bytes, bytearray)):
        return value.decode("utf-8")

    return str(value)


# ....................... #


def _to_bytes(value: str | bytes | Any) -> bytes:
    """Coerce a value to bytes, encoding strings as UTF-8."""

    if isinstance(value, bytes):
        return value

    return str(value).encode("utf-8")
