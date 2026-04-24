"""Integration tests for :class:`RedisSearchResultSnapshotAdapter`."""

from datetime import timedelta

import pytest

from forze.base.errors import CoreError
from forze_redis.adapters import RedisSearchResultSnapshotAdapter


@pytest.mark.integration
@pytest.mark.asyncio
async def test_put_run_empty_meta_only(
    redis_search_snapshot: RedisSearchResultSnapshotAdapter,
) -> None:
    await redis_search_snapshot.put_run(
        run_id="r1",
        fingerprint="fp",
        ordered_ids=[],
        ttl=timedelta(seconds=60),
        chunk_size=10,
    )
    assert await redis_search_snapshot.get_id_range("r1", 0, 5) == []
    m = await redis_search_snapshot.get_meta("r1")
    assert m is not None and m.complete and m.total == 0


@pytest.mark.integration
@pytest.mark.asyncio
async def test_put_run_and_get_id_range_single_chunk(
    redis_search_snapshot: RedisSearchResultSnapshotAdapter,
) -> None:
    ids = [f"id{i}" for i in range(5)]
    await redis_search_snapshot.put_run(
        run_id="r2",
        fingerprint="fp2",
        ordered_ids=ids,
        ttl=timedelta(seconds=60),
        chunk_size=100,
    )
    assert await redis_search_snapshot.get_id_range("r2", 0, 2) == ["id0", "id1"]
    assert await redis_search_snapshot.get_id_range("r2", 3, 10) == ["id3", "id4"]
    assert await redis_search_snapshot.get_id_range("r2", 5, 5) == []


@pytest.mark.integration
@pytest.mark.asyncio
async def test_put_run_multi_chunk_span(
    redis_search_snapshot: RedisSearchResultSnapshotAdapter,
) -> None:
    ids = [f"x{i}" for i in range(10)]
    await redis_search_snapshot.put_run(
        run_id="r3",
        fingerprint="fp3",
        ordered_ids=ids,
        ttl=timedelta(seconds=60),
        chunk_size=3,
    )
    assert await redis_search_snapshot.get_id_range("r3", 2, 4) == ["x2", "x3", "x4", "x5"]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_fingerprint_mismatch(
    redis_search_snapshot: RedisSearchResultSnapshotAdapter,
) -> None:
    await redis_search_snapshot.put_run(
        run_id="r4",
        fingerprint="secret",
        ordered_ids=["a", "b"],
        ttl=timedelta(seconds=60),
        chunk_size=10,
    )
    assert (
        await redis_search_snapshot.get_id_range(
            "r4", 0, 1, expected_fingerprint="other"
        )
        is None
    )
    assert await redis_search_snapshot.get_id_range("r4", 0, 1, expected_fingerprint="secret") == [
        "a"
    ]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_delete_run(
    redis_search_snapshot: RedisSearchResultSnapshotAdapter,
) -> None:
    await redis_search_snapshot.put_run(
        run_id="r5",
        fingerprint="f",
        ordered_ids=["p", "q"],
        ttl=timedelta(seconds=60),
        chunk_size=1,
    )
    await redis_search_snapshot.delete_run("r5")
    assert await redis_search_snapshot.get_meta("r5") is None
    assert await redis_search_snapshot.get_id_range("r5", 0, 1) is None


@pytest.mark.integration
@pytest.mark.asyncio
async def test_incomplete_run_get_range_none(
    redis_search_snapshot: RedisSearchResultSnapshotAdapter,
) -> None:
    await redis_search_snapshot.begin_run(
        run_id="r6",
        fingerprint="f6",
        chunk_size=2,
        ttl=timedelta(seconds=60),
    )
    assert await redis_search_snapshot.get_id_range("r6", 0, 1) is None
    await redis_search_snapshot.append_chunk(
        run_id="r6", chunk_index=0, ids=["a", "b"], is_last=True
    )
    assert await redis_search_snapshot.get_id_range("r6", 0, 2) == ["a", "b"]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_put_run_uses_adapter_default_ttl_and_chunk(
    redis_search_snapshot: RedisSearchResultSnapshotAdapter,
) -> None:
    """Omitting ``ttl`` and ``chunk_size`` uses the adapter's defaults (5 min, 5k chunk)."""
    await redis_search_snapshot.put_run(
        run_id="r8",
        fingerprint="f8",
        ordered_ids=["m1", "m2"],
    )
    assert await redis_search_snapshot.get_id_range("r8", 0, 2) == ["m1", "m2"]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_append_chunk_wrong_index(
    redis_search_snapshot: RedisSearchResultSnapshotAdapter,
) -> None:
    await redis_search_snapshot.begin_run(
        run_id="r7",
        fingerprint="f7",
        chunk_size=2,
        ttl=timedelta(seconds=60),
    )
    with pytest.raises(CoreError, match="expected chunk_index"):
        await redis_search_snapshot.append_chunk(
            run_id="r7", chunk_index=1, ids=["x"], is_last=True
        )
