"""Integration tests for tenant-isolated Redis streams."""

from __future__ import annotations

from uuid import uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.tenancy import TenantIdentity
from forze_redis.adapters import RedisStreamAdapter, RedisStreamCodec
from forze_redis.kernel.client import RedisClient
from forze.base.serialization import PydanticModelCodec

pytestmark = pytest.mark.integration


class _Payload(BaseModel):
    value: str


@pytest.mark.asyncio
async def test_tenant_aware_stream_isolates_by_tenant(redis_client: RedisClient) -> None:
    tenant_a = uuid4()
    tenant_b = uuid4()
    logical = f"events_{uuid4().hex[:8]}"
    codec = RedisStreamCodec(payload_codec=PydanticModelCodec(_Payload))

    adapter_a = RedisStreamAdapter(
        client=redis_client,
        codec=codec,
        tenant_aware=True,
        tenant_provider=lambda: TenantIdentity(tenant_id=tenant_a),
    )
    adapter_b = RedisStreamAdapter(
        client=redis_client,
        codec=codec,
        tenant_aware=True,
        tenant_provider=lambda: TenantIdentity(tenant_id=tenant_b),
    )

    await adapter_a.append(logical, _Payload(value="tenant-a"))
    await adapter_b.append(logical, _Payload(value="tenant-b"))

    msgs_a = await adapter_a.read({logical: "0"}, limit=10)
    msgs_b = await adapter_b.read({logical: "0"}, limit=10)

    assert len(msgs_a) == 1
    assert msgs_a[0].payload.value == "tenant-a"
    assert len(msgs_b) == 1
    assert msgs_b[0].payload.value == "tenant-b"
