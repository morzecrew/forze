"""Unit tests for tenant-aware Redis stream key wiring."""

from __future__ import annotations

from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from pydantic import BaseModel

pytest.importorskip("redis")

from forze.application.contracts.tenancy import TenantIdentity
from forze.base.serialization import PydanticModelCodec
from forze_redis.adapters.codecs import RedisStreamCodec
from forze_redis.adapters.stream import RedisStreamAdapter, _stream_physical, _stream_wire_and_back
from forze_redis.kernel.client import RedisClient


class _Payload(BaseModel):
    n: int


@pytest.fixture
def codec() -> RedisStreamCodec[_Payload]:
    return RedisStreamCodec(payload_codec=PydanticModelCodec(_Payload))


def test_stream_wire_and_back_prefixes_tenant() -> None:
    tenant_id = uuid4()
    adapter = RedisStreamAdapter(
        client=Mock(spec=RedisClient),
        codec=RedisStreamCodec(payload_codec=PydanticModelCodec(_Payload)),
        tenant_aware=True,
        tenant_provider=lambda: TenantIdentity(tenant_id=tenant_id),
    )

    wired, back = _stream_wire_and_back(adapter, {"events": "0"})

    assert wired == {f"tenant:{tenant_id}:stream:events": "0"}
    assert back[f"tenant:{tenant_id}:stream:events"] == "events"


def test_stream_physical_without_tenant() -> None:
    adapter = RedisStreamAdapter(
        client=Mock(spec=RedisClient),
        codec=RedisStreamCodec(payload_codec=PydanticModelCodec(_Payload)),
        tenant_aware=False,
    )
    assert _stream_physical(adapter, "events") == "events"


@pytest.mark.asyncio
async def test_append_uses_physical_stream_key(codec: RedisStreamCodec[_Payload]) -> None:
    tenant_id = uuid4()
    client = Mock(spec=RedisClient)
    client.xadd = AsyncMock(return_value="0-1")
    adapter = RedisStreamAdapter(
        client=client,
        codec=codec,
        tenant_aware=True,
        tenant_provider=lambda: TenantIdentity(tenant_id=tenant_id),
    )

    await adapter.append("events", _Payload(n=1))

    client.xadd.assert_awaited_once()
    assert client.xadd.await_args[0][0] == f"tenant:{tenant_id}:stream:events"
