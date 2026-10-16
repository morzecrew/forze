"""The periodic connection-hygiene lifecycle steps — ticks, health, and validation.

# covers: forze_socketio.connection_lifecycle (backplane heartbeat, expiry/presence steps)
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import MagicMock

import pytest
from socketio.async_server import AsyncServer

from forze.application.contracts.execution import LifecycleStep
from forze.application.execution import ExecutionContext, ExecutionRuntime
from forze.application.execution.deps import DepsRegistry
from forze.base.exceptions import CoreException
from forze_kits.integrations.realtime import realtime_stream_spec
from forze_mock import MockDepsModule
from forze_redis.adapters.realtime_presence import RedisRealtimePresence
from forze_redis.kernel.client import RoutedRedisClient
from forze_socketio import (
    BackplaneHealth,
    InMemoryRealtimePresence,
    RealtimeGateway,
    StreamGroupSignalSource,
    attach_realtime_connection,
    realtime_backplane_heartbeat_lifecycle_step,
    realtime_gateway_lifecycle_step,
    realtime_identity_expiry_lifecycle_step,
    realtime_presence_heartbeat_lifecycle_step,
)

# ----------------------- #


class _StubManager:
    def __init__(self, *, thread: Any = None) -> None:
        self.thread = thread

    def get_participants(self, namespace: str, room: str | None) -> Any:
        yield from ()


class _StubSio:
    def __init__(self, *, fail_emits: bool = False, thread: Any = None) -> None:
        self.manager = _StubManager(thread=thread)
        self.fail_emits = fail_emits
        self.emits = 0

    async def emit(self, event: str, data: Any = None, **_: Any) -> None:
        if self.fail_emits:
            raise RuntimeError("backplane down")

        self.emits += 1


def _ctx() -> ExecutionContext:
    return cast(
        ExecutionContext,
        SimpleNamespace(drainables=SimpleNamespace(register=lambda loop: None)),
    )


async def _run_ticks(step, *, until, timeout: float = 5.0) -> None:  # type: ignore[no-untyped-def]
    ctx = _ctx()
    await step.startup(ctx)

    waited = 0.0
    while not until() and waited < timeout:
        await asyncio.sleep(0.01)
        waited += 0.01

    await step.shutdown(ctx)


# ----------------------- #


async def test_backplane_heartbeat_records_ok() -> None:
    sio = _StubSio()
    health = BackplaneHealth()
    step = realtime_backplane_heartbeat_lifecycle_step(
        cast(Any, sio), health, interval=timedelta(milliseconds=10)
    )

    await _run_ticks(step, until=lambda: health.last_ok_at is not None and sio.emits >= 2)

    assert health.consecutive_failures == 0
    assert health.seconds_since_ok >= 0.0


async def test_backplane_heartbeat_records_publish_failures() -> None:
    sio = _StubSio(fail_emits=True)
    health = BackplaneHealth()
    step = realtime_backplane_heartbeat_lifecycle_step(
        cast(Any, sio), health, interval=timedelta(milliseconds=10)
    )

    await _run_ticks(step, until=lambda: health.consecutive_failures >= 2)

    assert health.consecutive_failures >= 2
    assert health.last_ok_at is None  # never succeeded — the -1 "wiring" signal


async def test_backplane_heartbeat_detects_dead_listener() -> None:
    class _DeadListener:
        def done(self) -> bool:
            return True

    # publishes succeed, but the manager's delivery task has exited — still unhealthy
    sio = _StubSio(thread=_DeadListener())
    health = BackplaneHealth()
    step = realtime_backplane_heartbeat_lifecycle_step(
        cast(Any, sio), health, interval=timedelta(milliseconds=10)
    )

    await _run_ticks(step, until=lambda: health.consecutive_failures >= 1)

    assert health.consecutive_failures >= 1
    assert health.last_ok_at is None


async def test_expiry_and_presence_steps_tick_with_no_connections() -> None:
    sio = _StubSio()
    presence = InMemoryRealtimePresence()

    expiry = realtime_identity_expiry_lifecycle_step(
        cast(Any, sio), interval=timedelta(milliseconds=10)
    )
    heartbeat = realtime_presence_heartbeat_lifecycle_step(
        cast(Any, sio), presence, interval=timedelta(milliseconds=10)
    )

    for step in (expiry, heartbeat):
        ctx = _ctx()
        await step.startup(ctx)
        await step.startup(ctx)  # duplicate startup is ignored, not a second task
        await asyncio.sleep(0.05)
        await step.shutdown(ctx)

        task = step.startup.task  # type: ignore[attr-defined]
        assert task is not None and task.done() and not task.cancelled()


async def test_non_positive_interval_is_refused() -> None:
    sio = _StubSio()

    with pytest.raises(CoreException):
        realtime_backplane_heartbeat_lifecycle_step(
            cast(Any, sio), BackplaneHealth(), interval=timedelta(0)
        )

    with pytest.raises(CoreException):
        realtime_identity_expiry_lifecycle_step(cast(Any, sio), interval=timedelta(seconds=-1))

# ....................... #


def _routed_presence() -> RedisRealtimePresence:
    """Redis presence on a client routed per tenant, with no tenant to route by."""

    routed = RoutedRedisClient(
        secrets=MagicMock(), secret_ref_for_tenant={}, tenant_provider=lambda: None
    )

    return RedisRealtimePresence(client=routed)


def test_the_presence_heartbeat_refuses_a_presence_store_routed_per_tenant() -> None:
    # The heartbeat runs with no tenant bound; rooms carry their tenant, so the store is
    # meant to be tenant-global. Routed per tenant, every refresh failed and the TTL then
    # expired this node's live connections.
    with pytest.raises(CoreException) as caught:
        realtime_presence_heartbeat_lifecycle_step(cast(Any, _StubSio()), _routed_presence())

    assert caught.value.code == "tenant_global_required"


def test_the_connect_handlers_refuse_a_presence_store_routed_per_tenant() -> None:
    # The connect join and the disconnect leave run with no tenant bound, so every connect
    # failed on such a store.
    async def resolve(_connect: Any) -> Any:  # pragma: no cover - refused before any connect
        raise AssertionError("connected")

    with pytest.raises(CoreException) as caught:
        attach_realtime_connection(AsyncServer(), resolve=resolve, presence=_routed_presence())

    assert caught.value.code == "tenant_global_required"


def _gateway_step(*, mailboxed: bool) -> LifecycleStep:
    gateway = RealtimeGateway(
        sio=cast(Any, _StubSio()),
        source=StreamGroupSignalSource(stream_spec=realtime_stream_spec()),
        presence=_routed_presence(),
        mailbox_factory=(lambda _ctx: cast(Any, None)) if mailboxed else None,
    )

    return realtime_gateway_lifecycle_step(gateway)


async def test_the_gateway_step_refuses_a_presence_store_routed_per_tenant() -> None:
    # The live path counts a mailboxed signal's principal room with no tenant bound.
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(MockDepsModule()).freeze())

    async with runtime.scope():
        with pytest.raises(CoreException) as caught:
            await _gateway_step(mailboxed=True).startup(runtime.get_context())

    assert caught.value.code == "tenant_global_required"


async def test_a_gateway_without_a_mailbox_never_reads_presence_and_starts() -> None:
    step = _gateway_step(mailboxed=False)
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(MockDepsModule()).freeze())

    async with runtime.scope():
        ctx = runtime.get_context()
        await step.startup(ctx)

        assert ctx.drainables.loops
        assert step.shutdown is not None
        await step.shutdown(ctx)
