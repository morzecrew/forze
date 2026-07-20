"""Unit tests for Redis stream adapters (with mocked client)."""

from datetime import timedelta
from unittest.mock import AsyncMock, Mock

import pytest
from pydantic import BaseModel

pytest.importorskip("redis")

from forze.base.serialization import PydanticModelCodec
from forze_redis.adapters.codecs import RedisStreamCodec
from forze_redis.adapters.stream import RedisStreamAdapter, RedisStreamGroupAdapter
from forze_redis.kernel.client import RedisClient


class _Payload(BaseModel):
    n: int


@pytest.fixture
def codec() -> RedisStreamCodec[_Payload]:
    return RedisStreamCodec(payload_codec=PydanticModelCodec(_Payload))


@pytest.mark.asyncio
async def test_stream_adapter_read_decodes_entries(codec: RedisStreamCodec[_Payload]) -> None:
    client = Mock(spec=RedisClient)
    client.xread = AsyncMock(
        return_value=[
            (
                "events",
                [
                    (
                        "0-1",
                        {b"payload": b'{"n":7}', b"type": b"evt"},
                    ),
                ],
            ),
        ],
    )
    adapter = RedisStreamAdapter(client=client, codec=codec)

    out = await adapter.read({"events": "0"}, limit=5, timeout=timedelta(seconds=2))

    client.xread.assert_awaited_once_with(
        {"events": "0"},
        count=5,
        block_ms=2000,
    )
    assert len(out) == 1
    assert out[0].stream == "events"
    assert out[0].id == "0-1"
    assert out[0].payload.n == 7
    assert out[0].type == "evt"


@pytest.mark.asyncio
async def test_stream_adapter_read_without_timeout(codec: RedisStreamCodec[_Payload]) -> None:
    client = Mock(spec=RedisClient)
    client.xread = AsyncMock(return_value=[])
    adapter = RedisStreamAdapter(client=client, codec=codec)

    await adapter.read({"s": "0"})

    client.xread.assert_awaited_once_with({"s": "0"}, count=None, block_ms=None)


@pytest.mark.asyncio
async def test_stream_adapter_append_encodes_and_xadd(codec: RedisStreamCodec[_Payload]) -> None:
    client = Mock(spec=RedisClient)
    client.xadd = AsyncMock(return_value="0-2")
    adapter = RedisStreamAdapter(client=client, codec=codec)

    msg_id = await adapter.append(
        "events",
        _Payload(n=3),
        type="created",
        key="k",
    )

    assert msg_id == "0-2"
    client.xadd.assert_awaited_once()
    call_kw = client.xadd.await_args
    assert call_kw[0][0] == "events"
    assert call_kw[0][1]["payload"] == '{"n":3}'
    assert call_kw[0][1]["type"] == "created"
    assert call_kw[0][1]["key"] == "k"


@pytest.mark.asyncio
async def test_stream_group_adapter_read_and_ack(codec: RedisStreamCodec[_Payload]) -> None:
    client = Mock(spec=RedisClient)
    client.xgroup_read = AsyncMock(
        return_value=[
            (
                "jobs",
                [("0-3", {b"payload": b'{"n":1}'})],
            ),
        ],
    )
    client.xack = AsyncMock(return_value=1)
    adapter = RedisStreamGroupAdapter(client=client, codec=codec)

    msgs = await adapter.read(
        "g1",
        "c1",
        {"jobs": ">"},
        limit=10,
        timeout=None,
    )

    assert len(msgs) == 1
    assert msgs[0].stream == "jobs"
    assert msgs[0].payload.n == 1

    client.xgroup_read.assert_awaited_once_with(
        group="g1",
        consumer="c1",
        streams={"jobs": ">"},
        count=10,
        block_ms=None,
        noack=False,
    )

    n = await adapter.ack("g1", "jobs", ["0-3"])
    assert n == 1
    client.xack.assert_awaited_once_with("jobs", "g1", ["0-3"])


@pytest.mark.asyncio
async def test_stream_group_adapter_claim_loops_cursor_until_exhaustion(
    codec: RedisStreamCodec[_Payload],
) -> None:
    """claim() converts idle to ms and follows the XAUTOCLAIM cursor to 0-0."""

    client = Mock(spec=RedisClient)
    client.xautoclaim = AsyncMock(
        side_effect=[
            ("5-0", [("1-0", {b"payload": b'{"n":1}'}), ("2-0", {b"payload": b'{"n":2}'})], []),
            ("0-0", [("6-0", {b"payload": b'{"n":6}'})], ["3-0"]),
        ],
    )
    adapter = RedisStreamGroupAdapter(client=client, codec=codec)

    msgs = await adapter.claim("g1", "c2", "jobs", idle=timedelta(seconds=2))

    assert [m.id for m in msgs] == ["1-0", "2-0", "6-0"]
    assert [m.payload.n for m in msgs] == [1, 2, 6]
    assert all(m.stream == "jobs" for m in msgs)

    assert client.xautoclaim.await_count == 2
    first, second = client.xautoclaim.await_args_list
    assert first.args == ("jobs", "g1", "c2")
    assert first.kwargs == {"min_idle_ms": 2000, "start_id": "0-0", "count": None}
    assert second.kwargs == {"min_idle_ms": 2000, "start_id": "5-0", "count": None}


@pytest.mark.asyncio
async def test_stream_group_adapter_claim_stops_at_limit(
    codec: RedisStreamCodec[_Payload],
) -> None:
    """claim() requests only the remaining budget per page and stops once filled."""

    client = Mock(spec=RedisClient)
    client.xautoclaim = AsyncMock(
        side_effect=[
            ("4-0", [("1-0", {b"payload": b'{"n":1}'})], []),
            ("9-0", [("5-0", {b"payload": b'{"n":5}'}), ("6-0", {b"payload": b'{"n":6}'})], []),
        ],
    )
    adapter = RedisStreamGroupAdapter(client=client, codec=codec)

    msgs = await adapter.claim("g1", "c2", "jobs", idle=timedelta(milliseconds=500), limit=3)

    assert [m.id for m in msgs] == ["1-0", "5-0", "6-0"]

    # Cursor 9-0 is not exhausted, but the limit is met: no third call.
    assert client.xautoclaim.await_count == 2
    first, second = client.xautoclaim.await_args_list
    assert first.kwargs == {"min_idle_ms": 500, "start_id": "0-0", "count": 3}
    assert second.kwargs == {"min_idle_ms": 500, "start_id": "4-0", "count": 2}


@pytest.mark.asyncio
async def test_stream_group_adapter_pending_maps_rows(
    codec: RedisStreamCodec[_Payload],
) -> None:
    """pending() maps XPENDING rows into PendingEntry with timedelta idle."""

    client = Mock(spec=RedisClient)
    client.xpending = AsyncMock(return_value=[("1-0", "a", 1500, 2), ("2-0", "b", 30, 1)])
    adapter = RedisStreamGroupAdapter(client=client, codec=codec)

    rows = await adapter.pending("g1", "jobs", limit=5)

    client.xpending.assert_awaited_once_with("jobs", "g1", count=5, start_id="-")
    assert [
        (r.id, r.consumer, r.idle, r.delivery_count) for r in rows
    ] == [
        ("1-0", "a", timedelta(milliseconds=1500), 2),
        ("2-0", "b", timedelta(milliseconds=30), 1),
    ]


@pytest.mark.asyncio
async def test_stream_group_adapter_pending_pages_with_exclusive_cursor(
    codec: RedisStreamCodec[_Payload],
) -> None:
    """Unbounded pending() walks full pages and advances an exclusive ( cursor."""

    page_one = [(f"1-{i}", "a", 1000, 1) for i in range(100)]
    page_two = [("2-0", "a", 1000, 1)]

    client = Mock(spec=RedisClient)
    client.xpending = AsyncMock(side_effect=[page_one, page_two])
    adapter = RedisStreamGroupAdapter(client=client, codec=codec)

    rows = await adapter.pending("g1", "jobs")

    assert len(rows) == 101
    assert rows[-1].id == "2-0"

    assert client.xpending.await_count == 2
    first, second = client.xpending.await_args_list
    assert first.kwargs == {"count": 100, "start_id": "-"}
    assert second.kwargs == {"count": 100, "start_id": "(1-99"}
