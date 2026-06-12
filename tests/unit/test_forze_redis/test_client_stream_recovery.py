"""Unit tests for the ``xautoclaim`` / ``xpending`` client methods (stubbed redis-py).

Covers argument mapping onto the underlying ``redis-py`` calls and wire-shape
normalisation (bytes cursors/ids/fields, pre-Redis-7 two-element ``XAUTOCLAIM``
responses, trimmed-entry placeholders).  The pipeline-guard classification of
both methods lives in ``test_client_pipeline_guard.py``.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

pytest.importorskip("redis")

from forze_redis.kernel.client import RedisClient

# ----------------------- #


@pytest.fixture
def redis_client() -> RedisClient:
    client = RedisClient()
    client._RedisClient__client = MagicMock()  # type: ignore[attr-defined]
    return client


# ....................... #
# xautoclaim


@pytest.mark.asyncio
async def test_xautoclaim_maps_args_and_parses_response(
    redis_client: RedisClient,
) -> None:
    inner = redis_client._RedisClient__require_client()  # type: ignore[attr-defined]
    inner.xautoclaim = AsyncMock(
        return_value=[
            b"3-0",
            [
                (b"1-0", {b"payload": b'{"n":1}'}),
                (b"2-0", {b"payload": b'{"n":2}'}),
            ],
            [b"1-5"],
        ],
    )

    cursor, entries, deleted = await redis_client.xautoclaim(
        "s",
        "g",
        "c",
        min_idle_ms=60000,
        start_id="0-0",
        count=10,
    )

    inner.xautoclaim.assert_awaited_once_with(
        "s",
        "g",
        "c",
        min_idle_time=60000,
        start_id="0-0",
        count=10,
    )
    assert cursor == "3-0"
    assert entries == [
        ("1-0", {b"payload": b'{"n":1}'}),
        ("2-0", {b"payload": b'{"n":2}'}),
    ]
    assert deleted == ["1-5"]


@pytest.mark.asyncio
async def test_xautoclaim_defaults_start_at_zero_without_count(
    redis_client: RedisClient,
) -> None:
    inner = redis_client._RedisClient__require_client()  # type: ignore[attr-defined]
    inner.xautoclaim = AsyncMock(return_value=[b"0-0", [], []])

    cursor, entries, deleted = await redis_client.xautoclaim(
        "s",
        "g",
        "c",
        min_idle_ms=0,
    )

    inner.xautoclaim.assert_awaited_once_with(
        "s",
        "g",
        "c",
        min_idle_time=0,
        start_id="0-0",
        count=None,
    )
    assert (cursor, entries, deleted) == ("0-0", [], [])


@pytest.mark.asyncio
async def test_xautoclaim_tolerates_pre_redis7_and_trimmed_placeholders(
    redis_client: RedisClient,
) -> None:
    """Two-element responses (no deleted array) and ``(None, None)`` entries parse."""

    inner = redis_client._RedisClient__require_client()  # type: ignore[attr-defined]
    inner.xautoclaim = AsyncMock(
        return_value=[
            b"7-0",
            [
                (None, None),
                (b"5-0", {b"payload": b'{"n":5}'}),
            ],
        ],
    )

    cursor, entries, deleted = await redis_client.xautoclaim(
        "s",
        "g",
        "c",
        min_idle_ms=1000,
    )

    assert cursor == "7-0"
    assert entries == [("5-0", {b"payload": b'{"n":5}'})]
    assert deleted == []


# ....................... #
# xpending


@pytest.mark.asyncio
async def test_xpending_maps_args_and_parses_rows(redis_client: RedisClient) -> None:
    inner = redis_client._RedisClient__require_client()  # type: ignore[attr-defined]
    inner.xpending_range = AsyncMock(
        return_value=[
            {
                "message_id": b"1-0",
                "consumer": b"a",
                "time_since_delivered": 1500,
                "times_delivered": 1,
            },
            {
                "message_id": b"2-0",
                "consumer": b"b",
                "time_since_delivered": 200,
                "times_delivered": 3,
            },
        ],
    )

    rows = await redis_client.xpending("s", "g", count=50, start_id="(1-0")

    inner.xpending_range.assert_awaited_once_with(
        "s",
        "g",
        min="(1-0",
        max="+",
        count=50,
    )
    assert rows == [
        ("1-0", "a", 1500, 1),
        ("2-0", "b", 200, 3),
    ]


@pytest.mark.asyncio
async def test_xpending_defaults_full_range_and_handles_empty(
    redis_client: RedisClient,
) -> None:
    inner = redis_client._RedisClient__require_client()  # type: ignore[attr-defined]
    inner.xpending_range = AsyncMock(return_value=[])

    rows = await redis_client.xpending("s", "g", count=10)

    inner.xpending_range.assert_awaited_once_with(
        "s",
        "g",
        min="-",
        max="+",
        count=10,
    )
    assert rows == []
