"""Module-wired Redis stream & pub-sub round-trips.

Resolves the generic transport ports through ``RedisDepsModule`` (not hand-built adapters)
against a real Redis, proving the production wiring works end-to-end.
"""

from __future__ import annotations

import asyncio
from contextlib import suppress
from datetime import timedelta
from uuid import uuid4

import pytest

pytest.importorskip("redis")

from pydantic import BaseModel

from forze.application.contracts.pubsub import (
    PubSubCommandDepKey,
    PubSubQueryDepKey,
    PubSubSpec,
)
from forze.application.contracts.stream import (
    StreamCommandDepKey,
    StreamGroupAdminDepKey,
    StreamGroupQueryDepKey,
    StreamSpec,
)
from forze.application.execution import DepsRegistry, ExecutionRuntime
from forze.base.serialization import PydanticModelCodec
from forze_redis import (
    RedisDepsModule,
    RedisPubSubConfig,
    RedisStreamConfig,
    RedisStreamGroupConfig,
)
from forze_redis.kernel.client import RedisClient

pytestmark = pytest.mark.integration


class _Payload(BaseModel):
    value: str


_CODEC = PydanticModelCodec(_Payload)


@pytest.mark.asyncio
async def test_module_wired_stream_group_roundtrip(redis_client: RedisClient) -> None:
    channel = f"it:mod:stream:{uuid4().hex[:10]}"
    spec = StreamSpec(name=channel, codec=_CODEC)
    module = RedisDepsModule(
        client=redis_client,
        streams={channel: RedisStreamConfig(tenant_aware=False)},
        stream_groups={channel: RedisStreamGroupConfig(tenant_aware=False)},
    )
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(module).freeze())

    async with runtime.scope():
        ctx = runtime.get_context()

        admin = ctx.deps.resolve_configurable(
            ctx, StreamGroupAdminDepKey, spec, route=spec.name
        )
        await admin.ensure_group("g", channel, start_id="0")

        cmd = ctx.deps.resolve_configurable(ctx, StreamCommandDepKey, spec, route=spec.name)
        await cmd.append(channel, _Payload(value="one"))
        await cmd.append(channel, _Payload(value="two"))

        grp = ctx.deps.resolve_configurable(
            ctx, StreamGroupQueryDepKey, spec, route=spec.name
        )
        messages = await grp.read("g", "c1", {channel: ">"}, limit=10)

        assert {m.payload.value for m in messages} == {"one", "two"}
        assert await grp.ack("g", channel, [m.id for m in messages]) == 2


@pytest.mark.asyncio
async def test_module_wired_pubsub_roundtrip(redis_client: RedisClient) -> None:
    topic = f"it:mod:pubsub:{uuid4().hex[:10]}"
    spec = PubSubSpec(name=topic, codec=_CODEC)
    module = RedisDepsModule(
        client=redis_client,
        pubsub={topic: RedisPubSubConfig(tenant_aware=False)},
    )
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(module).freeze())

    async with runtime.scope():
        ctx = runtime.get_context()

        query = ctx.deps.resolve_configurable(ctx, PubSubQueryDepKey, spec, route=spec.name)
        cmd = ctx.deps.resolve_configurable(ctx, PubSubCommandDepKey, spec, route=spec.name)

        stream = query.subscribe([topic], timeout=timedelta(seconds=2))
        recv = asyncio.create_task(anext(stream))

        try:
            received = None
            for _ in range(5):
                await cmd.publish(topic, _Payload(value="hi"))
                try:
                    received = await asyncio.wait_for(asyncio.shield(recv), timeout=0.5)
                    break
                except asyncio.TimeoutError:
                    continue

            assert received is not None, "pub-sub message not received in time"
            assert received.payload.value == "hi"

        finally:
            if not recv.done():
                recv.cancel()
                with suppress(asyncio.CancelledError):
                    await recv
            await stream.aclose()
