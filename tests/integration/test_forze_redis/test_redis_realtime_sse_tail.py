"""Real-Redis integration for the SSE live tail — broadcast fan-out over a plain stream.

The SSE live leg deliberately uses the plain (non-group) stream read: every node sees
every signal, with zero consumer-group lifecycle. This validates the actual loop
(`_tail_to_hub`) against real Redis: the pre-existing backlog is fast-forwarded past,
new signals reach every subscribed hub, and the tenant/event-id headers survive into
the hub fan-out — the proof that a second transport is just another plane consumer.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any
from uuid import uuid4

import pytest

pytest.importorskip("redis")
pytest.importorskip("fastapi")

from forze.application.contracts.envelope import HEADER_EVENT_ID, HEADER_TENANT_ID
from forze.application.contracts.realtime import Audience, RealtimeSignal
from forze.application.contracts.stream import StreamSpec
from forze.base.serialization import PydanticModelCodec
from forze_fastapi.realtime import RealtimeSseHub
from forze_fastapi.realtime.lifecycle import _tail_to_hub  # pyright: ignore[reportPrivateUsage]
from forze_redis.adapters import RedisStreamAdapter, RedisStreamCodec
from forze_redis.kernel.client import RedisClient

pytestmark = pytest.mark.integration


class _AdapterDeps:
    """Resolve the real Redis adapter for whatever configurable key the loop asks for."""

    def __init__(self, adapter: RedisStreamAdapter) -> None:
        self._adapter = adapter

    def resolve_configurable(self, ctx: Any, key: Any, spec: Any, *, route: Any) -> Any:
        del ctx, key, spec, route
        return self._adapter


class _AdapterCtx:
    def __init__(self, adapter: RedisStreamAdapter) -> None:
        self.deps = _AdapterDeps(adapter)


@pytest.mark.asyncio
async def test_sse_tail_fast_forwards_then_fans_out_new_signals(
    redis_client: RedisClient,
) -> None:
    codec = RedisStreamCodec(payload_codec=PydanticModelCodec(RealtimeSignal))
    adapter = RedisStreamAdapter(client=redis_client, codec=codec)

    stream_name = f"it:realtime-sse:{uuid4().hex[:12]}"
    spec = StreamSpec(name=stream_name, codec=PydanticModelCodec(model_type=RealtimeSignal))
    tenant = uuid4()

    stale = RealtimeSignal.of(Audience.topic("room"), "e", {"n": 0})
    await adapter.append(stream_name, stale, type="e")
    await adapter.append(stream_name, stale, type="e")

    hub = RealtimeSseHub()
    tenanted = hub.subscribe(principal="nobody", tenant=tenant, topics=frozenset({"room"}))
    untenanted = hub.subscribe(principal="nobody", tenant=None, topics=frozenset({"room"}))
    stop = asyncio.Event()

    task = asyncio.create_task(
        _tail_to_hub(
            _AdapterCtx(adapter),  # type: ignore[arg-type]
            hub=hub,
            stream_spec=spec,
            batch=16,
            poll_interval=timedelta(milliseconds=100),
            stop=stop,
        )
    )

    try:
        await asyncio.sleep(0.5)  # fast-forward completes against real Redis
        assert tenanted.queue.empty() and untenanted.queue.empty()

        live = RealtimeSignal.of(Audience.topic("room"), "e", {"n": 1})
        await adapter.append(
            stream_name,
            live,
            type="e",
            headers={HEADER_TENANT_ID: str(tenant), HEADER_EVENT_ID: "evt-1"},
        )

        signal, event_id = await asyncio.wait_for(tenanted.queue.get(), timeout=10)
        assert signal.payload == {"n": 1}
        assert event_id == "evt-1"  # the durable id survives into the fan-out
        assert untenanted.queue.empty()  # tenant scoping holds on the real substrate

    finally:
        stop.set()
        await asyncio.wait_for(task, timeout=10)
