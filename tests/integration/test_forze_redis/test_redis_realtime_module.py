"""Realtime egress over module-wired Redis.

Proves the production path: signals published through the module's ``StreamCommandDepKey``
are consumed by a real ``RealtimeGateway`` whose ``StreamGroupSignalSource`` resolves the
group ports from ``RedisDepsModule`` (not hand-built adapters) and emitted to clients.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any
from uuid import uuid4

import pytest

pytest.importorskip("redis")

from forze.application.contracts.realtime import Audience, RealtimeSignal
from forze.application.contracts.stream import (
    StreamCommandDepKey,
    AckStreamGroupAdminDepKey,
)
from forze.application.execution import DepsRegistry, ExecutionRuntime
from forze_kits.integrations.realtime import realtime_stream_spec
from forze_redis import RedisDepsModule, RedisStreamConfig, RedisStreamGroupConfig
from forze_redis.kernel.client import RedisClient
from forze_socketio import RealtimeGateway, StreamGroupSignalSource

pytestmark = pytest.mark.integration


class _StubSio:
    def __init__(self) -> None:
        self.emitted: list[int] = []

    async def emit(
        self,
        event: str,
        data: Any = None,
        *,
        namespace: str | None = None,
        room: str | None = None,
        **_: Any,
    ) -> None:
        self.emitted.append(data["data"]["n"])  # uniform {id, data} envelope


@pytest.mark.asyncio
async def test_realtime_egress_over_module_wired_redis(redis_client: RedisClient) -> None:
    channel = f"it:rt:mod:{uuid4().hex[:10]}"
    spec = realtime_stream_spec(channel)
    runtime = ExecutionRuntime(
        deps=DepsRegistry.from_modules(
            RedisDepsModule(
                client=redis_client,
                streams={channel: RedisStreamConfig(tenant_aware=False)},
                stream_groups={channel: RedisStreamGroupConfig(tenant_aware=False)},
            )
        ).freeze()
    )

    sio = _StubSio()
    n = 8

    async with runtime.scope():
        ctx = runtime.get_context()

        # provision the consumer group (control plane) and publish (data plane) — both via the module
        admin = ctx.deps.resolve_configurable(
            ctx, AckStreamGroupAdminDepKey, spec, route=spec.name
        )
        await admin.ensure_group("realtime-gateway", channel, start_id="0")

        cmd = ctx.deps.resolve_configurable(ctx, StreamCommandDepKey, spec, route=spec.name)
        for i in range(n):
            await cmd.append(
                channel, RealtimeSignal.of(Audience.topic("room"), "ping", {"n": i})
            )

        gateway = RealtimeGateway(
            sio=sio,  # type: ignore[arg-type]
            source=StreamGroupSignalSource(
                stream_spec=spec,
                group="realtime-gateway",
                consumer="gw-1",
                poll_interval=timedelta(seconds=0.05),
                reclaim_idle=None,
            ),
        )
        task = asyncio.create_task(gateway.run(ctx))
        try:
            for _ in range(200):
                await asyncio.sleep(0.02)
                if len(sio.emitted) >= n:
                    break
            await asyncio.sleep(0.1)  # settle
        finally:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task

    assert sorted(sio.emitted) == list(range(n))
