"""Three socket.io replicas sharing a Redis fleet rebalance behind a least-loaded balancer.

# covers: forze_socketio.rebalance (connection_rebalance_lifecycle_step, connection_report)

Each replica is python-socketio's own server, with Engine.IO sockets stood in by a fake
whose close fires the library's disconnect and hands the client back to the balancer. The
balancer reconnects it, after a short backoff, to the replica holding the fewest transports,
as a least-connections balancer would. The fleet, its beats and its views are real Redis.
"""

from __future__ import annotations

import asyncio
import itertools
from collections.abc import AsyncIterator
from datetime import timedelta
from typing import Any
from uuid import uuid4

import pytest
import pytest_asyncio
from socketio.async_server import AsyncServer
from testcontainers.redis import RedisContainer

from forze.application.contracts.fleet import FleetSpec
from forze.application.execution import build_runtime
from forze.application.execution.runtime import ExecutionRuntime
from forze_kits.integrations.fleet import fleet_membership_lifecycle_step
from forze_redis import RedisDepsModule, RedisFleetConfig
from forze_redis.kernel.client import RedisClient, RedisConfig
from forze_socketio import connection_rebalance_lifecycle_step, connection_report

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

# ----------------------- #

SPEC = FleetSpec(name="gateways", ttl=timedelta(seconds=1))
NAMESPACES = ("/lobby", "/game")
EVERY = timedelta(milliseconds=200)


def _lobby(namespace: str, sid: str) -> bool:
    return namespace == "/lobby"


class _Socket:
    """An Engine.IO socket stand-in whose close hands its client back to the balancer."""

    def __init__(
        self, balancer: _Balancer, server: AsyncServer, eio_sid: str, namespaces: tuple[str, ...]
    ) -> None:
        self.balancer = balancer
        self.server = server
        self.sid = eio_sid
        self.namespaces = namespaces
        self.queue: asyncio.Queue[Any] = asyncio.Queue()
        self.closed = False

    async def close(self, wait: bool = True, abort: bool = False, reason: Any = None) -> None:
        await self.server.eio._trigger_event("disconnect", self.sid, reason, run_async=False)
        self.closed = True
        self.balancer.reconnect(self.namespaces)


class _Balancer:
    """Least-connections: every connect goes to the replica with the fewest transports."""

    def __init__(self, servers: list[AsyncServer]) -> None:
        self.servers = servers
        self.ids = itertools.count()
        self.moved: list[tuple[str, ...]] = []
        self.pending: set[asyncio.Task[None]] = set()

    async def connect(self, server: AsyncServer, namespaces: tuple[str, ...]) -> None:
        eio_sid = f"eio-{next(self.ids)}"
        server.eio.sockets[eio_sid] = _Socket(self, server, eio_sid, namespaces)
        server.environ[eio_sid] = {}

        for namespace in namespaces:
            await server.manager.connect(eio_sid, namespace)

    def reconnect(self, namespaces: tuple[str, ...]) -> None:
        self.moved.append(namespaces)
        task = asyncio.create_task(self._reconnect(namespaces))
        self.pending.add(task)
        task.add_done_callback(self.pending.discard)

    async def _reconnect(self, namespaces: tuple[str, ...]) -> None:
        await asyncio.sleep(0.05)  # the client's backoff
        await self.connect(min(self.servers, key=lambda s: len(s.eio.sockets)), namespaces)


# ....................... #


@pytest_asyncio.fixture
async def client(redis_container: RedisContainer) -> AsyncIterator[RedisClient]:
    host = redis_container.get_container_host_ip()
    port = redis_container.get_exposed_port(6379)
    redis = RedisClient()
    # Six loops beat and read at once: room enough that none is refused a connection.
    await redis.initialize(dsn=f"redis://{host}:{port}/0", config=RedisConfig(max_size=20))

    yield redis

    await redis.close()


def _replica(client: RedisClient, namespace: str, sio: AsyncServer, me: str) -> ExecutionRuntime:
    return build_runtime(
        RedisDepsModule(
            client=client, fleets={str(SPEC.name): RedisFleetConfig(namespace=namespace)}
        ),
        lifecycle_steps=[
            fleet_membership_lifecycle_step(
                SPEC,
                every=timedelta(milliseconds=100),
                member_id=me,
                report=connection_report(sio, namespaces=NAMESPACES, movable=_lobby),
            ),
            connection_rebalance_lifecycle_step(
                sio,
                fleet=SPEC,
                every=EVERY,
                member_id=me,
                namespaces=NAMESPACES,
                movable=_lobby,
                per_minute=1200,
                settle=timedelta(milliseconds=500),
                cooldown=timedelta(milliseconds=500),
            ),
        ],
        drain_timeout=timedelta(0),
    )


# ....................... #


async def test_an_unbalanced_fleet_converges_without_moving_game_sockets(
    client: RedisClient,
) -> None:
    servers = [AsyncServer() for _ in range(3)]
    balancer = _Balancer(servers)

    # One replica restarted last: the first holds 50 lobby sockets and, among them, 10 in a
    # game (each also in the lobby); the others 5 each. 70 over three is a mean of 23.3.
    for index in range(60):
        await balancer.connect(servers[0], NAMESPACES if index % 6 == 0 else ("/lobby",))

    for server in servers[1:]:
        for _ in range(5):
            await balancer.connect(server, ("/lobby",))

    namespace = f"it:rebalance:{uuid4().hex[:12]}"
    runtimes = [_replica(client, namespace, sio, f"r{i}") for i, sio in enumerate(servers)]
    leave = asyncio.Event()

    async def serve(runtime: ExecutionRuntime) -> None:
        async with runtime.scope():
            await leave.wait()

    tasks = [asyncio.create_task(serve(runtime)) for runtime in runtimes]
    loads: list[int] = []

    try:
        # Bounded: 50 intervals of the rebalance tick, ten seconds.
        for _ in range(100):
            await asyncio.sleep(0.1)
            loads = [len(server.eio.sockets) for server in servers]

            if sum(loads) == 70 and max(loads) <= 70 / 3 * 1.15:
                break

        else:
            raise AssertionError(f"did not converge: {loads}")

    finally:
        leave.set()
        await asyncio.wait_for(asyncio.gather(*tasks), timeout=10)
        await asyncio.wait_for(asyncio.gather(*balancer.pending), timeout=5)

    # Only lobby-only sockets moved, and no more than the excess: 60 down to at most 26.
    assert balancer.moved
    assert all(namespaces == ("/lobby",) for namespaces in balancer.moved)
    assert 34 <= len(balancer.moved) <= 40
    assert (
        sum(1 for socket in servers[0].eio.sockets.values() if socket.namespaces == NAMESPACES)
        == 10
    )
