"""Real-Redis cross-node fan-out: a gateway emit on one node reaches a connection
held on **another** node via the Socket.IO Redis manager — the deployment property
the egress plane relies on (the gateway need not own the recipient's socket).

This isolates the fan-out leg; the stream-consumption + exactly-once leg is covered
by ``test_redis_realtime_multinode``. Here two ``AsyncServer`` s share one Redis
backplane: node A holds a (fake) connection in a room, node B's gateway emits to
that room, and we assert node A locally delivers the packet.
"""

from __future__ import annotations

import asyncio
from contextlib import suppress
from typing import Any
from uuid import UUID, uuid4

import pytest
from testcontainers.redis import RedisContainer

pytest.importorskip("redis")

from forze.application.contracts.realtime import Audience, RealtimeSignal
from forze.application.execution import ExecutionContext
from forze_socketio import (
    RealtimeGateway,
    RealtimeSignalSource,
    SignalHandler,
    build_socketio_server,
    room_for,
)

pytestmark = pytest.mark.integration

_TENANT = UUID("22222222-2222-2222-2222-222222222222")


def _dsn(redis_container: RedisContainer) -> str:
    host = redis_container.get_container_host_ip()
    port = redis_container.get_exposed_port(6379)
    return f"redis://{host}:{port}/0"


class _NullSource(RealtimeSignalSource):
    """A source that never produces — the gateway is driven via ``_emit`` directly."""

    async def run(self, ctx: ExecutionContext, handler: SignalHandler) -> None:  # pragma: no cover
        await asyncio.Event().wait()


async def _shutdown(server: Any) -> None:
    manager = server.manager
    task = getattr(manager, "thread", None)
    if task is not None:
        task.cancel()
        with suppress(asyncio.CancelledError, Exception):
            await task
    redis = getattr(manager, "redis", None)
    if redis is not None:
        with suppress(Exception):
            await redis.aclose()


@pytest.mark.asyncio
async def test_gateway_emit_reaches_connection_on_another_node(
    redis_container: RedisContainer,
) -> None:
    dsn = _dsn(redis_container)
    channel = f"sio:{uuid4().hex[:8]}"  # isolate this test's backplane

    audience = Audience.principal("u-1")
    room = room_for(audience, _TENANT)  # t:<tenant>:principal:u-1

    # node A — holds the live connection; redis-backed manager, no gateway.
    node_a = build_socketio_server(redis_url=dsn, redis_channel=channel)
    # node B — runs the gateway; a separate manager on the same backplane.
    node_b = build_socketio_server(redis_url=dsn, redis_channel=channel)

    delivered: list[Any] = []

    async def _capture(eio_sid: str, pkt: Any) -> None:
        delivered.append(pkt)

    # patch node A's local-delivery seam: the manager calls this per recipient sid.
    node_a._send_eio_packet = _capture  # type: ignore[method-assign]

    try:
        node_a.manager.initialize()  # start the pub/sub listener (subscribe)
        node_b.manager.initialize()

        # register a fake connection on node A and join it to the target room.
        node_a.manager.basic_enter_room("sid-a", "/", None, eio_sid="eio-a")
        node_a.manager.basic_enter_room("sid-a", "/", room, eio_sid="eio-a")

        await asyncio.sleep(1.0)  # let both managers finish subscribing

        gateway = RealtimeGateway(sio=node_b, source=_NullSource())
        signal = RealtimeSignal.of(audience, "order.shipped", {"text": "hi"})

        # emit repeatedly until the cross-node delivery lands (covers the pub/sub
        # subscribe race — each emit is an independent fan-out attempt).
        for _ in range(50):
            await gateway._emit(signal, _TENANT)
            await asyncio.sleep(0.1)
            if delivered:
                break

    finally:
        await _shutdown(node_a)
        await _shutdown(node_b)

    assert delivered, "gateway emit on node B never fanned out to node A"
    # the encoded Socket.IO packet carries the event addressed to node A's room.
    assert "order.shipped" in str(delivered[0].data)
