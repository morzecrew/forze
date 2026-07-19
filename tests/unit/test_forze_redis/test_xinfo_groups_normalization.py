"""``xinfo_groups`` row normalization — bytes-mode responses decode to the advertised shape.

# covers: RedisClient.xinfo_groups (key/value normalization)

A bytes-mode connection returns XINFO GROUPS rows with bytes field names and bytes text
values; the client's signature promises ``dict[str, object]``, and downstream floor math
(``depth``, ``trim_acknowledged``) keys on ``"name"``/``"pending"``/``"lag"``. Numeric
fields and the Redis ≥ 7 ``None`` lag must pass through untouched.
"""

from __future__ import annotations

from typing import Any

import pytest

pytest.importorskip("redis")

from forze_redis.kernel.client import RedisClient

# ----------------------- #


class _BytesModeRedis:
    async def xinfo_groups(self, stream: str) -> list[dict[Any, Any]]:
        return [
            {
                b"name": b"gw",
                b"consumers": 2,
                b"pending": 3,
                b"last-delivered-id": b"7-0",
                b"lag": None,  # Redis ≥ 7: unknown after a trim — must survive as None
            },
            {"name": "already-str", "pending": 0, "lag": 4},  # str-mode rows untouched
        ]


async def test_rows_are_normalized_to_string_keys_and_text_values() -> None:
    client = RedisClient()
    client._RedisClient__client = _BytesModeRedis()  # type: ignore[attr-defined]  # noqa: SLF001

    rows = await client.xinfo_groups("s")

    assert rows == [
        {
            "name": "gw",
            "consumers": 2,
            "pending": 3,
            "last-delivered-id": "7-0",
            "lag": None,
        },
        {"name": "already-str", "pending": 0, "lag": 4},
    ]
