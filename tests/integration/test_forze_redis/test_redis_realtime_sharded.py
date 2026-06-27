"""Real-Redis namespace-tier realtime streams — the substrate the
``TenantShardedSignalSource`` consumes.

Proves a **tenant-aware** Redis consumer group isolates realtime signals per tenant by
the ``tenant:{id}:stream:`` key prefix: two tenants share one *logical* stream name but
physically distinct keys, so each gateway shard's group reads only its tenant's signals —
with **no** ``forze_tenant_id`` header in play (the tenant is the stream's identity, set
by the producer's ambient tenant at write time). This is what lets the sharded source bind
the tenant from the stream and scope a tenant-aware mailbox by a *trusted* tenant.
"""

from __future__ import annotations

from uuid import UUID, uuid4

import pytest

pytest.importorskip("redis")

from forze.application.contracts.realtime import (
    DEFAULT_REALTIME_GROUP,
    Audience,
    RealtimeSignal,
)
from forze.application.contracts.tenancy import TenantIdentity
from forze.base.serialization import PydanticModelCodec
from forze_redis.adapters import (
    RedisStreamAdapter,
    RedisStreamCodec,
    RedisStreamGroupAdapter,
    RedisStreamGroupAdminAdapter,
)
from forze_redis.kernel.client import RedisClient

pytestmark = pytest.mark.integration

_T1 = UUID("11111111-1111-1111-1111-111111111111")
_T2 = UUID("22222222-2222-2222-2222-222222222222")


def _for_tenant(client: RedisClient, codec: RedisStreamCodec[RealtimeSignal], tenant: UUID):  # type: ignore[no-untyped-def]
    """A tenant-aware writer + consumer-group pair bound to *tenant* (its own key prefix)."""

    provider = lambda: TenantIdentity(tenant_id=tenant)  # noqa: E731
    writer = RedisStreamAdapter(client=client, codec=codec, tenant_aware=True, tenant_provider=provider)
    group = RedisStreamGroupAdapter(client=client, codec=codec, tenant_aware=True, tenant_provider=provider)
    admin = RedisStreamGroupAdminAdapter(client=client, tenant_aware=True, tenant_provider=provider)
    return writer, group, admin


@pytest.mark.asyncio
async def test_tenant_aware_consumer_group_isolates_realtime_signals(redis_client: RedisClient) -> None:
    codec = RedisStreamCodec(payload_codec=PydanticModelCodec(RealtimeSignal))
    stream = f"it:rt:{uuid4().hex[:12]}"  # one logical name; the prefix makes the keys distinct
    group, consumer = DEFAULT_REALTIME_GROUP, "gw-1"

    writer_a, group_a, admin_a = _for_tenant(redis_client, codec, _T1)
    writer_b, group_b, admin_b = _for_tenant(redis_client, codec, _T2)

    # each shard ensures its group on its own per-tenant key
    await admin_a.ensure_group(group, stream, start_id="$")
    await admin_b.ensure_group(group, stream, start_id="$")

    sig_a = RealtimeSignal.of(Audience.principal("u1"), "order.shipped", {"text": "a"})
    sig_b = RealtimeSignal.of(Audience.principal("u1"), "order.shipped", {"text": "b"})  # same principal
    await writer_a.append(stream, sig_a, type="order.shipped")  # no tenant header — the key isolates
    await writer_b.append(stream, sig_b, type="order.shipped")

    msgs_a = await group_a.read(group, consumer, {stream: ">"}, limit=10)
    msgs_b = await group_b.read(group, consumer, {stream: ">"}, limit=10)

    # each tenant's group sees only its own signal, despite the shared logical stream name
    assert [m.payload for m in msgs_a] == [sig_a]
    assert [m.payload for m in msgs_b] == [sig_b]

    # the groups are independent: acking T1 leaves T2 untouched
    assert await group_a.ack(group, stream, [msgs_a[0].id]) == 1
    assert await group_a.pending(group, stream) == []
    assert await group_b.pending(group, stream) != []  # T2 still pending, unaffected by T1's ack
