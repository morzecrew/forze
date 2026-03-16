"""Performance tests for MongoClient."""

from uuid import uuid4

import pytest

pytest.importorskip("pymongo")

from forze_mongo.kernel.platform import MongoClient


def _perf_collection(prefix: str) -> str:
    return f"perf_{prefix}_{uuid4().hex[:12]}"


@pytest.mark.perf
@pytest.mark.asyncio
async def test_health_benchmark(async_benchmark, mongo_client: MongoClient) -> None:
    """Benchmark Mongo health check (ping)."""

    async def run() -> None:
        status, ok = await mongo_client.health()
        assert ok

    await async_benchmark(run)


@pytest.mark.perf
@pytest.mark.asyncio
async def test_insert_one_benchmark(async_benchmark, mongo_client: MongoClient) -> None:
    """Benchmark single document insert."""
    coll_name = _perf_collection("insert")
    coll = mongo_client.collection(coll_name)

    async def run() -> None:
        await mongo_client.insert_one(coll, {"value": "bench", "idx": 1})
        await mongo_client.delete_many(coll, {})

    await async_benchmark(run)


@pytest.mark.perf
@pytest.mark.asyncio
async def test_insert_many_benchmark(
    async_benchmark, mongo_client: MongoClient
) -> None:
    """Benchmark batch insert of 20 documents."""
    coll_name = _perf_collection("insert_many")
    coll = mongo_client.collection(coll_name)

    async def run() -> None:
        docs = [{"value": f"bench-{i}", "idx": i} for i in range(20)]
        await mongo_client.insert_many(coll, docs)
        await mongo_client.delete_many(coll, {})

    await async_benchmark(run)


@pytest.mark.perf
@pytest.mark.asyncio
async def test_find_one_benchmark(async_benchmark, mongo_client: MongoClient) -> None:
    """Benchmark find_one (document pre-seeded)."""
    coll_name = _perf_collection("find_one")
    coll = mongo_client.collection(coll_name)
    await mongo_client.insert_one(coll, {"value": "bench", "idx": 42})

    async def run() -> None:
        doc = await mongo_client.find_one(coll, {"idx": 42})
        assert doc is not None
        assert doc["value"] == "bench"

    await async_benchmark(run)

    await mongo_client.delete_many(coll, {})


@pytest.mark.perf
@pytest.mark.asyncio
async def test_find_many_small_benchmark(
    async_benchmark, mongo_client: MongoClient
) -> None:
    """Benchmark find_many with a small result set (10 docs)."""
    coll_name = _perf_collection("find_many")
    coll = mongo_client.collection(coll_name)
    await mongo_client.insert_many(
        coll, [{"value": f"v{i}", "idx": i} for i in range(10)]
    )

    async def run() -> None:
        docs = await mongo_client.find_many(coll, {}, limit=10)
        assert len(docs) == 10

    await async_benchmark(run)

    await mongo_client.delete_many(coll, {})


@pytest.mark.perf
@pytest.mark.asyncio
async def test_find_many_medium_benchmark(
    async_benchmark, mongo_client: MongoClient
) -> None:
    """Benchmark find_many with a medium result set (100 docs)."""
    coll_name = _perf_collection("find_many_med")
    coll = mongo_client.collection(coll_name)
    await mongo_client.insert_many(
        coll, [{"value": f"v{i}", "idx": i} for i in range(100)]
    )

    async def run() -> None:
        docs = await mongo_client.find_many(coll, {}, limit=100)
        assert len(docs) == 100

    await async_benchmark(run)

    await mongo_client.delete_many(coll, {})


@pytest.mark.perf
@pytest.mark.asyncio
async def test_update_one_benchmark(async_benchmark, mongo_client: MongoClient) -> None:
    """Benchmark update_one."""
    coll_name = _perf_collection("update")
    coll = mongo_client.collection(coll_name)
    await mongo_client.insert_one(coll, {"value": "old", "idx": 1})

    async def run() -> None:
        await mongo_client.update_one(coll, {"idx": 1}, {"$set": {"value": "updated"}})

    await async_benchmark(run)

    await mongo_client.delete_many(coll, {})


@pytest.mark.perf
@pytest.mark.asyncio
async def test_delete_one_benchmark(async_benchmark, mongo_client: MongoClient) -> None:
    """Benchmark delete_one (document pre-seeded each iteration via insert)."""
    coll_name = _perf_collection("delete")
    coll = mongo_client.collection(coll_name)

    async def run() -> None:
        await mongo_client.insert_one(coll, {"value": "to_delete", "idx": 1})
        await mongo_client.delete_one(coll, {"idx": 1})

    await async_benchmark(run)


@pytest.mark.perf
@pytest.mark.asyncio
async def test_count_benchmark(async_benchmark, mongo_client: MongoClient) -> None:
    """Benchmark count_documents."""
    coll_name = _perf_collection("count")
    coll = mongo_client.collection(coll_name)
    await mongo_client.insert_many(
        coll, [{"value": f"v{i}", "idx": i} for i in range(50)]
    )

    async def run() -> None:
        n = await mongo_client.count(coll, {})
        assert n == 50

    await async_benchmark(run)

    await mongo_client.delete_many(coll, {})
