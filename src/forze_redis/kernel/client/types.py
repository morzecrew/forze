"""Low-level type aliases for Redis stream and pub/sub wire formats."""

from collections.abc import Mapping
from typing import Any

# ----------------------- #
# Raw redis-py stream shapes.
#
# These describe the wire shapes of ``XREAD`` / ``XREADGROUP`` / ``XAUTOCLAIM`` responses
# structurally, rather than importing ``redis.typing.XRead*`` — those aliases only exist in
# redis-py 8+ (the typed-response overhaul), so importing them breaks ``forze_redis`` on
# redis-py 7. The parsing in :mod:`.utils` inspects the shape at runtime, so a loose alias is
# sufficient (and version-portable).

RawRedisStreamEntry = tuple[Any, Any]
"""One raw stream entry from redis-py: ``(id, fields)`` (element types vary by RESP version)."""

RawRedisStreamMessages = list[RawRedisStreamEntry]
"""A per-stream list of raw entries (may itself be nested for XREADGROUP claim rows)."""

# ....................... #

RedisStreamFields = dict[bytes, bytes]
"""Field-value mapping of a single stream entry, both as raw bytes."""

RedisStreamEntry = tuple[str, RedisStreamFields]
"""A decoded stream entry: ``(message_id, fields)``."""

RedisStreamBatch = tuple[str, list[RedisStreamEntry]]
"""All entries read from a single stream: ``(stream_name, entries)``."""

RedisStreamResponse = list[RedisStreamBatch]
"""Parsed response from ``XREAD`` or ``XREADGROUP``, one batch per stream."""

RawRedisStreamResponse = list[Any] | dict[Any, Any] | None
"""Raw ``redis-py`` response before normalisation to :data:`RedisStreamResponse`.

RESP2 returns a list of ``(stream, messages)``; RESP3 returns a ``{stream: messages}`` dict.
Deliberately loose (``list``/``dict``) so it accepts redis-py's own ``XRead*`` return types
across versions; :mod:`.utils` inspects the shape at runtime."""

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
