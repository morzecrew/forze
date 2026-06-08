"""Broader coverage for in-memory mock ports (cache, storage, queue, stream, idempotency)."""

from forze.base.exceptions import CoreException
import asyncio
from datetime import timedelta

import pytest
from pydantic import BaseModel

from forze.application.contracts.idempotency import IdempotencyRecord
from forze.application.contracts.pubsub import PubSubSpec
from forze.application.contracts.queue import QueueSpec
from forze.application.contracts.stream.specs import StreamSpec
from forze.base.serialization import PydanticModelCodec
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

# ----------------------- #

class _Msg(BaseModel):
    body: str

@pytest.mark.asyncio
async def test_mock_cache_versioned_get_many_and_hard_delete() -> None:
    st = MockState()
    c = MockCacheAdapter(state=st, namespace="ns")
    await c.set("plain", 1)
    await c.set_versioned("vkey", "1", {"a": True})
    hits, misses = await c.get_many(["plain", "vkey", "missing"])
    assert hits["plain"] == 1
    assert hits["vkey"] == {"a": True}
    assert misses == ["missing"]
    await c.set_many_versioned({("vkey", "2"): {"b": 2}})
    assert await c.get("vkey") == {"b": 2}
    await c.delete("vkey", hard=True)
    assert await c.get("vkey") is None

@pytest.mark.asyncio
async def test_mock_storage_upload_download_list_delete() -> None:
    st = MockState()
    s = MockStorageAdapter(state=st, bucket="b1")
    from forze.application.contracts.storage import UploadedObject

    meta = await s.upload(
        UploadedObject(filename="f.txt", data=b"hello", description="d", prefix="pre"),
    )
    assert meta.size == 5
    got = await s.download(meta.key)
    assert got.data == b"hello"
    rows, total = await s.list(10, 0, prefix="pre")
    assert total >= 1
    await s.delete(meta.key)
    with pytest.raises(CoreException):
        await s.download(meta.key)

@pytest.mark.asyncio
async def test_mock_idempotency_begin_commit_and_conflict() -> None:
    st = MockState()
    idem = MockIdempotencyAdapter(state=st, namespace="idem")
    assert await idem.begin("op", None, "h") is None
    snap = IdempotencyRecord(result=b"ok")
    with pytest.raises(CoreException):
        await idem.commit("op", "k", "wrong", snap)
    assert await idem.begin("op", "k", "hash") is None
    await idem.commit("op", "k", "hash", snap)
    cached = await idem.begin("op", "k", "hash")
    assert cached == snap
    with pytest.raises(CoreException):
        await idem.begin("op", "k", "other-hash")

@pytest.mark.asyncio
async def test_mock_tx_manager_transaction_is_noop() -> None:
    tx = MockTxManagerAdapter()
    async with tx.transaction():
        pass

@pytest.mark.asyncio
async def test_mock_queue_receive_ack_nack_requeue() -> None:
    st = MockState()
    q = MockQueueAdapter(
        state=st,
        namespace="q",
        codec=QueueSpec(
            name="q", codec=PydanticModelCodec(model_type=_Msg)
        ).codec,
    )
    mid = await q.enqueue("jobs", _Msg(body="x"))
    batch = await q.receive("jobs", limit=1)
    assert batch[0].id == mid
    assert await q.ack("jobs", [mid]) == 1
    mid2 = await q.enqueue("jobs", _Msg(body="y"))
    _ = await q.receive("jobs", limit=1)
    assert await q.nack("jobs", [mid2], requeue=True) == 1
    again = await q.receive("jobs", limit=1)
    assert again[0].id == mid2

@pytest.mark.asyncio
async def test_mock_queue_delayed_enqueue_not_visible_until_delay() -> None:
    st = MockState()
    q = MockQueueAdapter(
        state=st,
        namespace="q-delay",
        codec=QueueSpec(
            name="q-delay", codec=PydanticModelCodec(model_type=_Msg)
        ).codec,
    )
    mid = await q.enqueue("jobs", _Msg(body="later"), delay=timedelta(hours=1))
    assert await q.receive("jobs", limit=1) == []
    with st.lock:
        entry = st.queues["q-delay"]["jobs"][0]
        object.__setattr__(entry, "visible_at", entry.visible_at - timedelta(hours=2))
    batch = await q.receive("jobs", limit=1)
    assert batch[0].id == mid


@pytest.mark.asyncio
async def test_mock_queue_enqueue_many() -> None:
    st = MockState()
    q = MockQueueAdapter(
        state=st,
        namespace="q2",
        codec=QueueSpec(
            name="q2", codec=PydanticModelCodec(model_type=_Msg)
        ).codec,
    )
    ids = await q.enqueue_many("q", [_Msg(body="a"), _Msg(body="b")])
    assert len(ids) == 2

@pytest.mark.asyncio
async def test_mock_stream_read_and_group_ack() -> None:
    st = MockState()
    sa = MockStreamAdapter(
        state=st,
        namespace="s",
        codec=StreamSpec(
            name="s", codec=PydanticModelCodec(model_type=_Msg)
        ).codec,
    )
    sg = MockStreamGroupAdapter(stream=sa, state=st, namespace="s")
    sid = await sa.append("events", _Msg(body="e1"))
    rows = await sa.read({"events": "0"}, limit=5)
    assert len(rows) == 1
    assert rows[0].id == sid
    n = await sg.ack("g", "events", [sid])
    assert n == 1

@pytest.mark.asyncio
async def test_mock_pubsub_subscribe_receives_new_messages() -> None:
    st = MockState()
    ps = MockPubSubAdapter(
        state=st,
        namespace="ps",
        codec=PubSubSpec(
            name="ps", codec=PydanticModelCodec(model_type=_Msg)
        ).codec,
    )
    sub = ps.subscribe(["t1"], timeout=timedelta(milliseconds=50))

    async def _publish_after_subscriber_waits() -> None:
        await asyncio.sleep(0.05)
        await ps.publish("t1", _Msg(body="hi"))

    pub = asyncio.create_task(_publish_after_subscriber_waits())
    msg = await asyncio.wait_for(anext(sub), timeout=2.0)
    await pub
    assert msg.payload.body == "hi"
