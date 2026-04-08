"""Unit tests for additional forze_mock adapters (cache, storage, queue, etc.)."""

import asyncio
from datetime import timedelta

import pytest
from pydantic import BaseModel

from forze.application.contracts.idempotency import IdempotencySnapshot
from forze.base.errors import ConflictError, NotFoundError
from forze_mock.adapters import (
    MockCacheAdapter,
    MockIdempotencyAdapter,
    MockPubSubAdapter,
    MockQueueAdapter,
    MockState,
    MockStorageAdapter,
    MockStreamAdapter,
    MockStreamGroupAdapter,
    MockTxManagerAdapter,
)


class _Msg(BaseModel):
    body: str


@pytest.mark.asyncio
async def test_mock_cache_plain_and_versioned_and_delete_hard() -> None:
    state = MockState()
    cache = MockCacheAdapter(state=state, namespace="ns")

    await cache.set("a", 1)
    assert await cache.get("a") == 1

    await cache.set_versioned("v", "1", {"x": 2})
    assert await cache.get("v") == {"x": 2}

    hits, misses = await cache.get_many(["a", "v", "missing"])
    assert hits["a"] == 1 and hits["v"] == {"x": 2}
    assert misses == ["missing"]

    await cache.set_many({"b": 3})
    assert await cache.get("b") == 3

    await cache.set_many_versioned({("v", "2"): {"x": 3}})
    assert await cache.get("v") == {"x": 3}

    await cache.delete("v", hard=False)
    assert await cache.get("v") is None
    await cache.set_versioned("v", "1", {"keep": True})
    await cache.delete("v", hard=True)
    assert await cache.get("v") is None

    await cache.delete_many(["a", "b"], hard=True)
    assert await cache.get("a") is None


@pytest.mark.asyncio
async def test_mock_idempotency_begin_commit_and_conflicts() -> None:
    state = MockState()
    idem = MockIdempotencyAdapter(state=state, namespace="ns")
    snap: IdempotencySnapshot = {
        "code": 200,
        "content_type": "application/json",
        "body": b"{}",
    }

    assert await idem.begin("op", None, "h") is None
    await idem.commit("op", None, "h", snap)

    assert await idem.begin("op", "k", "h1") is None
    await idem.commit("op", "k", "h1", snap)

    replay = await idem.begin("op", "k", "h1")
    assert replay == snap

    with pytest.raises(ConflictError, match="Payload hash mismatch"):
        await idem.begin("op", "k", "other")

    with pytest.raises(ConflictError, match="commit failed"):
        await idem.commit("op", "unknown", "h", snap)


@pytest.mark.asyncio
async def test_mock_storage_upload_download_list_delete() -> None:
    state = MockState()
    storage = MockStorageAdapter(state=state, bucket="files")

    obj = await storage.upload(
        "a.txt", b"hello", description="d", prefix="p"
    )
    assert obj["key"].startswith("p/")
    dl = await storage.download(obj["key"])
    assert dl["data"] == b"hello"
    assert dl["filename"] == "a.txt"

    rows, total = await storage.list(10, 0, prefix="p/")
    assert total >= 1
    assert any(r["key"] == obj["key"] for r in rows)

    await storage.delete(obj["key"])
    with pytest.raises(NotFoundError):
        await storage.download(obj["key"])


@pytest.mark.asyncio
async def test_mock_tx_manager_yields() -> None:
    tx = MockTxManagerAdapter()
    async with tx.transaction():
        assert True


@pytest.mark.asyncio
async def test_mock_queue_receive_ack_nack() -> None:
    state = MockState()
    q = MockQueueAdapter(state=state, namespace="q", model=_Msg)

    mid = await q.enqueue("jobs", _Msg(body="one"))
    batch = await q.receive("jobs", limit=1)
    assert len(batch) == 1
    assert await q.ack("jobs", [mid]) == 1

    mid2 = await q.enqueue("jobs", _Msg(body="two"))
    await q.receive("jobs")
    n = await q.nack("jobs", [mid2], requeue=True)
    assert n == 1
    again = await q.receive("jobs", limit=1)
    assert again[0]["payload"].body == "two"


@pytest.mark.asyncio
async def test_mock_queue_enqueue_many() -> None:
    state = MockState()
    q = MockQueueAdapter(state=state, namespace="q", model=_Msg)
    ids = await q.enqueue_many(
        "jobs", [_Msg(body="a"), _Msg(body="b")], enqueued_at=None
    )
    assert len(ids) == 2


@pytest.mark.asyncio
async def test_mock_pubsub_subscribe_emits_new_messages() -> None:
    state = MockState()
    ps = MockPubSubAdapter(state=state, namespace="ps", model=_Msg)

    async def run() -> None:
        await asyncio.sleep(0.06)
        await ps.publish("t1", _Msg(body="hi"))

    task = asyncio.create_task(run())
    got: list[str] = []
    async for msg in ps.subscribe(["t1"], timeout=timedelta(seconds=1)):
        got.append(msg["payload"].body)
        if len(got) >= 1:
            break
    await task
    assert got == ["hi"]


@pytest.mark.asyncio
async def test_mock_stream_read_tail_and_group() -> None:
    state = MockState()
    stream = MockStreamAdapter(state=state, namespace="s", model=_Msg)
    sid = await stream.append("s1", _Msg(body="a"))

    read = await stream.read({"s1": "stream-0"}, limit=10)
    assert len(read) == 1
    assert read[0]["id"] == sid

    group = MockStreamGroupAdapter(stream=stream, state=state, namespace="s")
    gread = await group.read("g", "c", {"s1": "stream-0"})
    assert len(gread) == 1

    n = await group.ack("g", "s1", [sid])
    assert n == 1
    n2 = await group.ack("g", "s1", [sid])
    assert n2 == 0

    async def collect_one() -> str:
        async for m in stream.tail({"s1": sid}, timeout=timedelta(seconds=0.2)):
            return m["payload"].body
        return ""

    append_task = asyncio.create_task(stream.append("s1", _Msg(body="tail")))
    body = await asyncio.wait_for(collect_one(), timeout=1.0)
    await append_task
    assert body == "tail"

    sid2 = await stream.append("s1", _Msg(body="after"))

    async def group_tail() -> str:
        async for m in group.tail(
            "g", "c", {"s1": sid2}, timeout=timedelta(seconds=0.2)
        ):
            return m["payload"].body
        return ""

    t2 = asyncio.create_task(stream.append("s1", _Msg(body="gt")))
    gb = await asyncio.wait_for(group_tail(), timeout=1.0)
    await t2
    assert gb == "gt"
