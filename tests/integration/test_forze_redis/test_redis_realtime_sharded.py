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

import asyncio
from datetime import timedelta
from uuid import UUID, uuid4

import pytest

pytest.importorskip("redis")

from forze.application.contracts.envelope import HEADER_EVENT_ID, HEADER_TENANT_ID
from forze.application.contracts.realtime import (
    DEFAULT_REALTIME_GROUP,
    Audience,
    RealtimeShard,
    RealtimeSignal,
)
from forze.application.contracts.stream import AckStreamGroupAdminDepKey, StreamCommandDepKey
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import DepsRegistry, ExecutionContext, ExecutionRuntime
from forze.application.execution.realtime import (
    GatewayDedup,
    RealtimeMailboxWriter,
    TenantShardedSignalSource,
)
from forze.base.exceptions import CoreException
from forze.base.primitives import HlcTimestamp
from forze.base.serialization import PydanticModelCodec
from forze.testing import context_from_modules
from forze_kits.integrations.realtime import (
    build_realtime_mailbox,
    realtime_inbox_spec,
    realtime_mailbox_spec,
    realtime_stream_spec,
)
from forze_mock import MockDepsModule, MockRouteConfig, MockState
from forze_redis import RedisDepsModule, RedisStreamConfig, RedisStreamGroupConfig
from forze_redis.adapters import (
    RedisStreamAdapter,
    RedisStreamCodec,
    RedisStreamGroupAdapter,
    RedisStreamGroupAdminAdapter,
)
from forze_redis.kernel.client import RedisClient
from tests.support.realtime_retention import UNSWEPT

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


# ....................... #


def _module_runtime(redis_client: RedisClient, channel: str, *, tenant_aware: bool) -> ExecutionRuntime:
    return ExecutionRuntime(
        deps=DepsRegistry.from_modules(
            RedisDepsModule(
                client=redis_client,
                streams={channel: RedisStreamConfig(tenant_aware=tenant_aware)},
                stream_groups={channel: RedisStreamGroupConfig(tenant_aware=tenant_aware)},
            )
        ).freeze()
    )


def _store_ctx() -> ExecutionContext:
    # dedup marks and mailboxes on the database side, per tenant (the production split)
    return context_from_modules(
        MockDepsModule(
            state=MockState(),
            routes={str(realtime_mailbox_spec().name): MockRouteConfig(tenant_aware=True)},
        )
    )


def _writer(store_ctx: ExecutionContext, channel: str) -> RealtimeMailboxWriter:
    shard = RealtimeShard(stream_spec=realtime_stream_spec(channel), tenants=(_T1, _T2))
    return RealtimeMailboxWriter(
        source=TenantShardedSignalSource(shard=shard, poll_interval=timedelta(milliseconds=20)),
        dedup=GatewayDedup(inbox_spec=realtime_inbox_spec(), tx_route="mock"),
        mailbox_factory=lambda _ctx: build_realtime_mailbox(store_ctx, retention=UNSWEPT),
    )


async def _append_both(ctx: ExecutionContext, channel: str) -> None:
    spec = realtime_stream_spec(channel)

    for n, tenant in enumerate((_T1, _T2), start=1):
        with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=tenant)):
            admin = ctx.deps.resolve_configurable(ctx, AckStreamGroupAdminDepKey, spec, route=spec.name)
            await admin.ensure_group(DEFAULT_REALTIME_GROUP, channel, start_id="0")
            cmd = ctx.deps.resolve_configurable(ctx, StreamCommandDepKey, spec, route=spec.name)
            signal = RealtimeSignal.of(Audience.principal("u1"), "order.shipped", {"text": f"for {n}"})
            await cmd.append(
                channel,
                signal,
                type=signal.event,
                headers={HEADER_EVENT_ID: str(UUID(int=n)), HEADER_TENANT_ID: str(tenant)},
            )


async def _texts(store_ctx: ExecutionContext, tenant: UUID) -> list[str]:
    with store_ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=tenant)):
        rows = await build_realtime_mailbox(store_ctx, retention=UNSWEPT).read_since(
            principal="u1", since=None
        )

    return [row.payload["text"] for row in rows]


@pytest.mark.asyncio
async def test_sharded_writer_stores_each_signal_in_its_tenants_mailbox_only(
    redis_client: RedisClient,
) -> None:
    channel = f"it:rt:shw:{uuid4().hex[:10]}"
    runtime = _module_runtime(redis_client, channel, tenant_aware=True)
    store_ctx = _store_ctx()
    writer = _writer(store_ctx, channel)

    async def handle(signal: RealtimeSignal, tenant: UUID | None, dedup_id: str | None, hlc: HlcTimestamp) -> None:
        # the source binds the tenant on its own context; the store side is a second one
        assert tenant is not None
        mailbox = build_realtime_mailbox(store_ctx, retention=UNSWEPT)
        with store_ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=tenant)):
            await writer._handle(store_ctx, mailbox, signal, tenant, dedup_id, hlc)  # pyright: ignore[reportPrivateUsage]

    async with runtime.scope():
        ctx = runtime.get_context()
        await _append_both(ctx, channel)

        stop = asyncio.Event()
        task = asyncio.create_task(writer.source.run(ctx, handle, stop=stop))

        for _ in range(250):
            if await _texts(store_ctx, _T1) and await _texts(store_ctx, _T2):
                break

            await asyncio.sleep(0.02)

        stop.set()
        await asyncio.wait_for(task, timeout=10)

    assert await _texts(store_ctx, _T1) == ["for 1"]
    assert await _texts(store_ctx, _T2) == ["for 2"]


@pytest.mark.asyncio
async def test_sharded_writer_over_a_tenant_global_stream_is_refused(
    redis_client: RedisClient,
) -> None:
    # One shared key: both tenant loops would read it and store each signal under their
    # own tenant. The writer refuses before reading, and neither mailbox gets a row.
    channel = f"it:rt:shg:{uuid4().hex[:10]}"
    runtime = _module_runtime(redis_client, channel, tenant_aware=False)
    store_ctx = _store_ctx()
    writer = _writer(store_ctx, channel)

    async with runtime.scope():
        ctx = runtime.get_context()
        await _append_both(ctx, channel)

        with pytest.raises(CoreException) as caught:
            await writer.run(ctx)

    assert caught.value.code == "tenant_aware_required"
    assert await _texts(store_ctx, _T1) == []
    assert await _texts(store_ctx, _T2) == []
