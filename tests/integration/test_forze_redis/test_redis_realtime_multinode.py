"""Real-Redis multi-node check: two gateways in one consumer group split the
load with exactly-once delivery (no double-emit) — the egress-plane scaling model.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any
from uuid import uuid4

import pytest

pytest.importorskip("redis")

from forze.application.contracts.deps import Deps
from forze.application.contracts.realtime import Audience, RealtimeSignal
from forze.application.contracts.stream import (
    AckStreamGroupAdminDepKey,
    AckStreamGroupQueryDepKey,
    StreamCommandDepKey,
)
from forze.application.execution import DepsRegistry, ExecutionRuntime
from forze.base.serialization import PydanticModelCodec
from forze_redis.adapters import (
    RedisStreamAdapter,
    RedisStreamCodec,
    RedisStreamGroupAdapter,
    RedisStreamGroupAdminAdapter,
)
from forze_redis.kernel.client import RedisClient
from forze_socketio import RealtimeGateway, StreamGroupSignalSource

pytestmark = pytest.mark.integration


class _StubSio:
    def __init__(self) -> None:
        self.emitted: list[int] = []

    async def emit(self, event: str, data: Any = None, *, namespace: str | None = None,
                   room: str | None = None, **_: Any) -> None:
        self.emitted.append(data["data"]["n"])  # uniform {id, data} envelope


def _redis_module(redis_client: RedisClient):  # type: ignore[no-untyped-def]
    codec = RedisStreamCodec(payload_codec=PydanticModelCodec(RealtimeSignal))
    writer = RedisStreamAdapter(client=redis_client, codec=codec)
    group = RedisStreamGroupAdapter(client=redis_client, codec=codec)
    admin = RedisStreamGroupAdminAdapter(client=redis_client)

    def module() -> Deps:
        return Deps.plain({
            StreamCommandDepKey: lambda _ctx, _spec: writer,
            AckStreamGroupQueryDepKey: lambda _ctx, _spec: group,
            AckStreamGroupAdminDepKey: lambda _ctx, _spec: admin,
        })

    return module


@pytest.mark.asyncio
async def test_two_gateways_share_load_exactly_once(redis_client: RedisClient) -> None:
    from forze_kits.integrations.realtime import realtime_stream_spec

    channel = f"it:rt:{uuid4().hex[:10]}"
    spec = realtime_stream_spec(channel)
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(_redis_module(redis_client)).freeze())

    sio1, sio2 = _StubSio(), _StubSio()
    fast = timedelta(seconds=0.05)

    def _gateway(sio: _StubSio, consumer: str) -> RealtimeGateway:
        return RealtimeGateway(
            sio=sio,  # type: ignore[arg-type]
            source=StreamGroupSignalSource(
                stream_spec=spec, group="realtime-gateway", consumer=consumer,
                poll_interval=fast, reclaim_idle=None,
            ),
        )

    n = 12
    async with runtime.scope():
        ctx = runtime.get_context()
        # create the group before publishing (start at the beginning so nothing is missed)
        admin = ctx.deps.resolve_configurable(ctx, AckStreamGroupAdminDepKey, spec, route=spec.name)
        await admin.ensure_group("realtime-gateway", channel, start_id="0")

        cmd = ctx.deps.resolve_configurable(ctx, StreamCommandDepKey, spec, route=spec.name)
        for i in range(n):
            await cmd.append(channel, RealtimeSignal.of(Audience.topic("room"), "ping", {"n": i}))

        t1 = asyncio.create_task(_gateway(sio1, "gw-1").run(ctx))
        t2 = asyncio.create_task(_gateway(sio2, "gw-2").run(ctx))
        try:
            for _ in range(200):
                await asyncio.sleep(0.02)
                if len(sio1.emitted) + len(sio2.emitted) >= n:
                    break
            await asyncio.sleep(0.1)  # settle
        finally:
            for t in (t1, t2):
                t.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await t

    delivered = sio1.emitted + sio2.emitted
    # the core multi-node property: every signal delivered exactly once across the
    # two consumers - none lost, none double-emitted. (Which consumer gets a given
    # entry is whoever-reads-first; an even split is not guaranteed by Redis.)
    assert sorted(delivered) == list(range(n))
    assert len(delivered) == len(set(delivered)) == n
