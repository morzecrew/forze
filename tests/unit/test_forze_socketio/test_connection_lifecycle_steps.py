"""The periodic connection-hygiene lifecycle steps — ticks, health, and validation.

# covers: forze_socketio.connection_lifecycle (backplane heartbeat, expiry/presence steps)
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from types import SimpleNamespace
from typing import Any, cast

import pytest

from forze.application.execution import ExecutionContext
from forze.base.exceptions import CoreException
from forze_socketio import (
    BackplaneHealth,
    InMemoryRealtimePresence,
    realtime_backplane_heartbeat_lifecycle_step,
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
