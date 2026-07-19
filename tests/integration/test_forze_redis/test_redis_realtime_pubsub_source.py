"""Real-Redis integration for the pubsub-backed signal source — broadcast live lane.

Validates ``PubSubSignalSource`` against real Redis pub/sub: a ``RealtimeSignal``
published on the channel reaches the bridged handler with the tenant header intact,
every subscribed node sees it (broadcast — the point of the lane), and the raced
stop exits the idle subscription cleanly.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any
from uuid import UUID, uuid4

import pytest

pytest.importorskip("redis")
pytest.importorskip("socketio")

from forze.application.contracts.envelope import HEADER_TENANT_ID
from forze.application.contracts.pubsub import PubSubSpec
from forze.application.contracts.realtime import Audience, RealtimeSignal
from forze.base.primitives import HlcTimestamp
from forze.base.serialization import PydanticModelCodec
from forze_redis.adapters import RedisPubSubAdapter, RedisPubSubCodec
from forze_redis.kernel.client import RedisClient
from forze_socketio import PubSubSignalSource

pytestmark = pytest.mark.integration


class _AdapterDeps:
    def __init__(self, adapter: RedisPubSubAdapter[RealtimeSignal]) -> None:
        self._adapter = adapter

    def resolve_configurable(self, ctx: Any, key: Any, spec: Any, *, route: Any) -> Any:
        del ctx, key, spec, route
        return self._adapter


class _AdapterCtx:
    def __init__(self, adapter: RedisPubSubAdapter[RealtimeSignal]) -> None:
        self.deps = _AdapterDeps(adapter)


@pytest.mark.asyncio
async def test_pubsub_source_broadcasts_to_every_subscribed_node(
    redis_client: RedisClient,
) -> None:
    adapter = RedisPubSubAdapter(
        client=redis_client,
        codec=RedisPubSubCodec(payload_codec=PydanticModelCodec(RealtimeSignal)),
    )

    channel = f"it:realtime-ps:{uuid4().hex[:12]}"
    spec = PubSubSpec(name=channel, codec=PydanticModelCodec(model_type=RealtimeSignal))
    tenant = uuid4()

    received_a: list[tuple[RealtimeSignal, UUID | None]] = []
    received_b: list[tuple[RealtimeSignal, UUID | None]] = []

    def _handler(into: list[tuple[RealtimeSignal, UUID | None]]) -> Any:
        async def handler(
            signal: RealtimeSignal, tenant: UUID | None, dedup_id: str | None, hlc: HlcTimestamp
        ) -> None:
            into.append((signal, tenant))

        return handler

    stop = asyncio.Event()
    source = PubSubSignalSource(pubsub_spec=spec, poll_interval=timedelta(milliseconds=100))

    # two "nodes" — every subscriber must see every signal (broadcast, not a group)
    node_a = asyncio.create_task(source.run(_AdapterCtx(adapter), _handler(received_a), stop=stop))  # type: ignore[arg-type]
    node_b = asyncio.create_task(source.run(_AdapterCtx(adapter), _handler(received_b), stop=stop))  # type: ignore[arg-type]

    try:
        await asyncio.sleep(0.5)  # both subscriptions live on the real broker

        signal = RealtimeSignal.of(Audience.topic("room"), "e", {"n": 1})
        await adapter.publish(
            channel, signal, type="e", headers={HEADER_TENANT_ID: str(tenant)}
        )

        async def _both() -> None:
            while not (received_a and received_b):
                await asyncio.sleep(0.02)

        await asyncio.wait_for(_both(), timeout=10)

        for received in (received_a, received_b):
            got, got_tenant = received[0]
            assert got.payload == {"n": 1}
            assert got_tenant == tenant  # the header survived the real broker

    finally:
        stop.set()
        await asyncio.wait_for(asyncio.gather(node_a, node_b), timeout=10)
