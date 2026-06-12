"""Low-level type aliases for Redis stream and pub/sub wire formats."""

from typing import Mapping

from redis.typing import XReadGroupResponse, XReadResponse

# ----------------------- #

RedisStreamFields = dict[bytes, bytes]
"""Field-value mapping of a single stream entry, both as raw bytes."""

RedisStreamEntry = tuple[str, RedisStreamFields]
"""A decoded stream entry: ``(message_id, fields)``."""

RedisStreamBatch = tuple[str, list[RedisStreamEntry]]
"""All entries read from a single stream: ``(stream_name, entries)``."""

RedisStreamResponse = list[RedisStreamBatch]
"""Parsed response from ``XREAD`` or ``XREADGROUP``, one batch per stream."""

RawRedisStreamResponse = XReadResponse | XReadGroupResponse | None
"""Raw ``redis-py`` response before normalisation to :data:`RedisStreamResponse`."""

RedisAutoClaimResponse = tuple[str, list[RedisStreamEntry], list[str]]
"""Parsed ``XAUTOCLAIM`` page: ``(next_cursor, claimed_entries, deleted_ids)``.

``next_cursor`` of ``"0-0"`` means the pending-entries list was fully scanned;
``deleted_ids`` are entries dropped from the PEL because they no longer exist
in the stream (empty on servers predating Redis 7).
"""

RedisPendingEntry = tuple[str, str, int, int]
"""Parsed extended ``XPENDING`` row: ``(message_id, consumer, idle_ms, delivery_count)``."""

# ....................... #

RedisPubSubMessage = tuple[str, bytes]
"""Parsed pub/sub message: ``(channel, payload_bytes)``."""

RawRedisPubSubMessage = Mapping[str, object]
"""Raw ``redis-py`` pub/sub dict before parsing."""
