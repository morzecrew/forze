"""Unit tests for :class:`RedisSearchResultSnapshotAdapter` (no I/O)."""

import pytest
from unittest.mock import AsyncMock

from forze.base.errors import CoreError
from forze_redis.adapters import RedisKeyCodec, RedisSearchResultSnapshotAdapter


@pytest.mark.unit
@pytest.mark.asyncio
async def test_get_id_range_rejects_zero_limit() -> None:
    adapter = RedisSearchResultSnapshotAdapter(
        client=AsyncMock(),
        key_codec=RedisKeyCodec(namespace="u:snap:unit"),
    )
    with pytest.raises(CoreError, match="limit"):
        await adapter.get_id_range("run", 0, 0)
