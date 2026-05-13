"""Unit tests for Redis stream adapters (with mocked client)."""

from datetime import timedelta
from unittest.mock import AsyncMock, Mock

import pytest
from pydantic import BaseModel

pytest.importorskip("redis")

from forze_redis.adapters.stream import RedisStreamAdapter, RedisStreamGroupAdapter
from forze_redis.adapters.codecs import RedisStreamCodec
from forze_redis.kernel.platform.client import RedisClient


class _Payload(BaseModel):
    n: int


@pytest.fixture
def codec() -> RedisStreamCodec[_Payload]:
    return RedisStreamCodec(model=_Payload)


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
    assert out[0]["stream"] == "events"
    assert out[0]["id"] == "0-1"
    assert out[0]["payload"].n == 7
    assert out[0]["type"] == "evt"


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
    assert msgs[0]["stream"] == "jobs"
    assert msgs[0]["payload"].n == 1

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
