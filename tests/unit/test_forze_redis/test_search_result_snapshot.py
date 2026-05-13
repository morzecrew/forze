"""Unit tests for :class:`RedisSearchResultSnapshotAdapter` (no I/O)."""

import json

import pytest
from unittest.mock import AsyncMock

from forze.base.errors import CoreError
from forze_redis.adapters import RedisKeyCodec, RedisSearchResultSnapshotAdapter


@pytest.mark.unit
@pytest.mark.asyncio
async def test_append_chunk_cas_conflict_raises() -> None:
    meta = {
        "fingerprint": "fp",
        "chunk_size": 2,
        "ttl_seconds": 60,
        "complete": False,
        "next_chunk_index": 0,
        "total_ids": 0,
    }
    raw_meta = json.dumps(meta).encode("utf-8")
    client = AsyncMock()
    client.get = AsyncMock(return_value=raw_meta)
    client.run_script = AsyncMock(return_value="0")

    adapter = RedisSearchResultSnapshotAdapter(
        client=client,
        key_codec=RedisKeyCodec(namespace="u:snap:cas"),
    )

    with pytest.raises(CoreError, match="Concurrent snapshot append"):
        await adapter.append_chunk(
            run_id="r1",
            chunk_index=0,
            ids=["a", "b"],
            is_last=False,
        )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_append_chunk_cas_success_calls_script() -> None:
    meta = {
        "fingerprint": "fp",
        "chunk_size": 2,
        "ttl_seconds": 60,
        "complete": False,
        "next_chunk_index": 0,
        "total_ids": 0,
    }
    raw_meta = json.dumps(meta).encode("utf-8")
    client = AsyncMock()
    client.get = AsyncMock(return_value=raw_meta)
    client.run_script = AsyncMock(return_value="1")

    adapter = RedisSearchResultSnapshotAdapter(
        client=client,
        key_codec=RedisKeyCodec(namespace="u:snap:cas"),
    )

    await adapter.append_chunk(
        run_id="r1",
        chunk_index=0,
        ids=["a", "b"],
        is_last=False,
    )

    assert client.run_script.await_count == 1
    rc = client.run_script.await_args
    assert rc is not None
    assert rc.args[2][0] == raw_meta


@pytest.mark.unit
@pytest.mark.asyncio
async def test_get_id_range_rejects_zero_limit() -> None:
    adapter = RedisSearchResultSnapshotAdapter(
        client=AsyncMock(),
        key_codec=RedisKeyCodec(namespace="u:snap:unit"),
    )
    with pytest.raises(CoreError, match="limit"):
        await adapter.get_id_range("run", 0, 0)
