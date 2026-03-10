"""Low-level type aliases for Redis stream and pub/sub wire formats."""

from typing import Mapping, Optional, Sequence

# ----------------------- #

RedisStreamFields = dict[bytes, bytes]
"""Field-value mapping of a single stream entry, both as raw bytes."""

RedisStreamEntry = tuple[str, RedisStreamFields]
"""A decoded stream entry: ``(message_id, fields)``."""

RedisStreamBatch = tuple[str, list[RedisStreamEntry]]
"""All entries read from a single stream: ``(stream_name, entries)``."""

RedisStreamResponse = list[RedisStreamBatch]
"""Parsed response from ``XREAD`` or ``XREADGROUP``, one batch per stream."""

RawRedisStreamResponse = Optional[
    Sequence[
        tuple[
            str | bytes,
            list[tuple[str | bytes, object]],
        ]
    ]
]
"""Raw ``redis-py`` response before normalisation to :data:`RedisStreamResponse`."""

# ....................... #

RedisPubSubMessage = tuple[str, bytes]
"""Parsed pub/sub message: ``(channel, payload_bytes)``."""

RawRedisPubSubMessage = Mapping[str, object]
"""Raw ``redis-py`` pub/sub dict before parsing."""
