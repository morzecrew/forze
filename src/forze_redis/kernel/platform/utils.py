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
    if raw is None or not raw:
        return []

    out: RedisStreamResponse = []

    for stream_raw, messages in raw:
        stream = (
            stream_raw.decode("utf-8")
            if isinstance(stream_raw, (bytes, bytearray))
            else str(stream_raw)
        )

        parsed_messages: list[RedisStreamEntry] = []

        for msg_id_raw, data_raw in messages:
            msg_id = (
                msg_id_raw.decode("utf-8")
                if isinstance(msg_id_raw, (bytes, bytearray))
                else str(msg_id_raw)
            )

            if isinstance(data_raw, dict):
                data_dict = data_raw  # pyright: ignore[reportUnknownVariableType]

            else:
                data_dict = dict(data_raw)  # type: ignore[call-overload]

            normalized: RedisStreamFields = {}

            for k, v in data_dict.items():  # pyright: ignore[reportUnknownVariableType]
                key = (
                    k
                    if isinstance(k, bytes)
                    else str(k).encode(  # pyright: ignore[reportUnknownArgumentType]
                        "utf-8"
                    )
                )

                if isinstance(v, bytes):
                    value = v

                else:
                    value = str(v).encode(  # pyright: ignore[reportUnknownArgumentType]
                        "utf-8"
                    )

                normalized[key] = value

            parsed_messages.append((msg_id, normalized))

        out.append((stream, parsed_messages))

    return out


# ....................... #


def parse_pubsub_message(raw: RawRedisPubSubMessage) -> RedisPubSubMessage | None:
    msg_type = raw.get("type")

    if msg_type not in {"message", b"message"}:
        return None

    channel_raw = raw.get("channel")
    data_raw = raw.get("data")

    if channel_raw is None or data_raw is None:
        return None

    channel = (
        channel_raw.decode("utf-8")
        if isinstance(channel_raw, (bytes, bytearray))
        else str(channel_raw)
    )
    data = (
        data_raw
        if isinstance(data_raw, bytes)
        else str(data_raw).encode("utf-8")  # pyright: ignore[reportUnknownArgumentType]
    )

    return channel, data
