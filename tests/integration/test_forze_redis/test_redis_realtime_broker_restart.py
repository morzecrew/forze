"""The supervised realtime gateway across a broker restart — the outage-resilience leg.

Kills and restarts the Redis container **while the gateway's consume loop is live**, then
proves delivery resumes without operator intervention: the loop absorbs the connection errors
during the outage (or the supervisor restarts it — either recovery path is legal), the client
pool reconnects, and signals appended after the restart are delivered. The container runs with
AOF persistence, so the stream, the consumer group, and the pre-restart acks survive — nothing
already delivered is re-emitted.

Uses its **own** container (not the shared session one): restarting the fixture every other
test in the directory depends on would be sabotage, not isolation.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any

import pytest
import pytest_asyncio

pytest.importorskip("redis")

from collections.abc import AsyncIterator, Iterator

from testcontainers.redis import RedisContainer

from forze.application.contracts.deps import Deps
from forze.application.contracts.realtime import Audience, RealtimeSignal
from forze.application.contracts.stream import (
    AckStreamGroupAdminDepKey,
    AckStreamGroupQueryDepKey,
    StreamSpec,
)
from forze.application.execution import DepsRegistry, ExecutionRuntime
from forze.base.serialization import PydanticModelCodec
from forze_redis.adapters import (
    RedisStreamAdapter,
    RedisStreamCodec,
    RedisStreamGroupAdapter,
    RedisStreamGroupAdminAdapter,
)
from forze_redis.kernel.client import RedisClient, RedisConfig
from forze_socketio import RealtimeGateway, StreamGroupSignalSource, realtime_gateway_lifecycle_step

# ----------------------- #

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

_GROUP = "restart-gw"
_STREAM = "it-restart-rt"


class _RecordingSio:
    def __init__(self) -> None:
        self.payloads: list[dict[str, Any]] = []

    async def emit(self, event: str, data: Any = None, **_: Any) -> None:
        self.payloads.append(data["data"])


def _free_host_port() -> int:
    """Reserve an ephemeral host port for a **fixed** binding.

    A container published on a random port gets a *new* one on ``docker restart`` — the
    reconnecting client would keep dialing the dead port forever. A fixed binding survives
    the restart, which is the whole point of this file.
    """

    import socket

    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))

        return int(probe.getsockname()[1])


@pytest.fixture(scope="function")
def restartable_redis() -> Iterator[RedisContainer]:
    """A dedicated AOF-persistent container this test is free to restart."""

    container = RedisContainer(image="valkey/valkey:9.0").with_command(
        "valkey-server --appendonly yes"
    )
    container.with_bind_ports(6379, _free_host_port())

    with container:
        yield container


@pytest_asyncio.fixture(scope="function")
async def restartable_client(restartable_redis: RedisContainer) -> AsyncIterator[RedisClient]:
    host = restartable_redis.get_container_host_ip()
    port = restartable_redis.get_exposed_port(6379)

    client = RedisClient()
    await client.initialize(dsn=f"redis://{host}:{port}/0", config=RedisConfig(max_size=5))

    yield client

    await client.close()


def _runtime(client: RedisClient) -> ExecutionRuntime:
    codec = RedisStreamCodec(payload_codec=PydanticModelCodec(RealtimeSignal))
    group = RedisStreamGroupAdapter(client=client, codec=codec)
    admin = RedisStreamGroupAdminAdapter(client=client)

    def module() -> Deps:
        return Deps.plain(
            {
                AckStreamGroupQueryDepKey: lambda _ctx, _spec: group,
                AckStreamGroupAdminDepKey: lambda _ctx, _spec: admin,
            }
        )

    return ExecutionRuntime(deps=DepsRegistry.from_modules(module).freeze())


async def _append_with_retry(
    writer: RedisStreamAdapter[RealtimeSignal], signal: RealtimeSignal, *, timeout: float
) -> None:
    """Append, retrying while the broker is still coming back up."""

    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout

    while True:
        try:
            await writer.append(_STREAM, signal, type=signal.event)
            return

        except Exception:
            if loop.time() >= deadline:
                raise

            await asyncio.sleep(0.2)


async def _wait_for(predicate, *, timeout: float) -> None:  # type: ignore[no-untyped-def]
    waited = 0.0
    while not predicate() and waited < timeout:
        await asyncio.sleep(0.05)
        waited += 0.05


# ----------------------- #


async def test_gateway_survives_broker_restart_and_resumes_delivery(
    restartable_redis: RedisContainer, restartable_client: RedisClient
) -> None:
    codec = RedisStreamCodec(payload_codec=PydanticModelCodec(RealtimeSignal))
    writer = RedisStreamAdapter(client=restartable_client, codec=codec)
    admin = RedisStreamGroupAdminAdapter(client=restartable_client)
    await admin.ensure_group(_GROUP, _STREAM, start_id="0")

    spec: StreamSpec[RealtimeSignal] = StreamSpec(
        name=_STREAM, codec=PydanticModelCodec(model_type=RealtimeSignal)
    )
    sio = _RecordingSio()
    gateway = RealtimeGateway(
        sio=sio,  # pyright: ignore[reportArgumentType]
        source=StreamGroupSignalSource(
            stream_spec=spec,
            group=_GROUP,
            poll_interval=timedelta(milliseconds=100),
            reclaim_idle=timedelta(seconds=1),
        ),
    )
    step = realtime_gateway_lifecycle_step(gateway, restart_backoff=timedelta(milliseconds=100))

    runtime = _runtime(restartable_client)

    async with runtime.scope():
        ctx = runtime.get_context()
        await step.startup(ctx)

        # Phase 1: normal delivery, then everything acked.
        for i in range(3):
            await writer.append(
                _STREAM,
                RealtimeSignal.of(Audience.topic("t"), "e", {"seq": i}),
                type="e",
            )

        await _wait_for(lambda: len(sio.payloads) >= 3, timeout=10)
        assert len(sio.payloads) == 3

        # Phase 2: the broker dies and comes back — mid-consume, no warning. AOF preserves
        # the stream, the group, and the acks; the loop rides out the outage.
        restartable_redis.get_wrapped_container().restart(timeout=10)

        for i in range(3, 6):
            await _append_with_retry(
                writer,
                RealtimeSignal.of(Audience.topic("t"), "e", {"seq": i}),
                timeout=30,
            )

        await _wait_for(lambda: len(sio.payloads) >= 6, timeout=30)

        # Everything delivered exactly once across the outage: the post-restart signals
        # arrived, and the pre-restart acks survived (no re-emit of 0..2).
        assert sorted(p["seq"] for p in sio.payloads) == [0, 1, 2, 3, 4, 5]

        await step.shutdown(ctx)

        task = step.startup.task  # type: ignore[attr-defined]
        assert task is not None and task.done() and not task.cancelled()
