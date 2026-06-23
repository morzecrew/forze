"""Periodic connection-hygiene steps — credential-expiry sweep and presence heartbeat.

Both are minimal background loops in the same spirit as
:mod:`forze_socketio.gateway_lifecycle`: own a task, cancel it on shutdown, no
restart/backoff (that's a future unified runner). Each node only sees its own
connections, so each node runs its own loop.
"""

from ._compat import require_socketio

require_socketio()

# ....................... #

import asyncio
from contextlib import suppress
from datetime import timedelta
from typing import Awaitable, Callable, final

import attrs
from socketio.async_server import AsyncServer

from forze.application.contracts.execution import LifecycleHook, LifecycleStep
from forze.application.execution import ExecutionContext
from forze.base.exceptions import exc
from forze.base.logging import Logger
from forze.base.primitives import StrKey

from ._logging import ForzeSocketIOLogger
from .connection import RealtimePresence, refresh_presence, sweep_expired_connections

# ----------------------- #

_logger = Logger(ForzeSocketIOLogger.ERRORS)


@final
@attrs.define(slots=True, kw_only=True)
class _PeriodicStartup(LifecycleHook):
    """Run *tick* every *interval* until cancelled; one bad tick can't kill the loop."""

    tick: Callable[[], Awaitable[None]]
    interval: timedelta
    label: str
    task: asyncio.Task[None] | None = attrs.field(default=None, init=False)

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:  # noqa: ARG002
        if self.task is not None and not self.task.done():
            return

        self.task = asyncio.create_task(self._loop(), name=self.label)

    # ....................... #

    async def _loop(self) -> None:
        delay = self.interval.total_seconds()

        while True:
            try:
                await self.tick()

            except asyncio.CancelledError:
                raise

            except Exception:  # noqa: BLE001 - a transient sweep error must not kill the loop
                _logger.critical_exception(f"Realtime {self.label} tick failed")

            await asyncio.sleep(delay)


# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class _PeriodicShutdown(LifecycleHook):
    """Cancel the periodic task with structured cancellation."""

    startup: _PeriodicStartup

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:  # noqa: ARG002
        task = self.startup.task

        if task is None:
            return

        task.cancel()

        with suppress(asyncio.CancelledError):
            await task


# ....................... #


def _periodic_step(
    *, tick: Callable[[], Awaitable[None]], interval: timedelta, label: str, step_id: StrKey
) -> LifecycleStep:
    if interval.total_seconds() <= 0:
        # A non-positive interval would spin the worker (sleep(0) hot-loop) — fail at wiring.
        raise exc.configuration(f"{label} interval must be positive")

    startup = _PeriodicStartup(tick=tick, interval=interval, label=label)

    return LifecycleStep(
        id=step_id,
        startup=startup,
        shutdown=_PeriodicShutdown(startup=startup),
        requires_long_running=True,
    )


# ----------------------- #


def realtime_identity_expiry_lifecycle_step(
    sio: AsyncServer,
    *,
    namespace: str = "/",
    interval: timedelta = timedelta(seconds=30),
    step_id: StrKey = "realtime_identity_expiry",
) -> LifecycleStep:
    """Periodically disconnect connections whose credential has expired.

    Identity is bound once at connect; this re-checks ``RealtimeConnection.expires_at``
    on an interval so a long-lived socket can't outlive its credential. Set
    ``expires_at`` when resolving the connection for this to have any effect.
    """

    async def _tick() -> None:
        await sweep_expired_connections(sio, namespace=namespace)

    return _periodic_step(
        tick=_tick, interval=interval, label="identity_expiry", step_id=step_id
    )


# ....................... #


def realtime_presence_heartbeat_lifecycle_step(
    sio: AsyncServer,
    presence: RealtimePresence,
    *,
    namespace: str = "/",
    interval: timedelta = timedelta(seconds=30),
    step_id: StrKey = "realtime_presence_heartbeat",
) -> LifecycleStep:
    """Periodically re-assert presence for this node's connections.

    Required by a TTL-backed presence store (e.g. ``RedisRealtimePresence``): the
    TTL expires a crashed node's rows, so live connections must heartbeat. Use an
    *interval* comfortably shorter than the store's TTL. Harmless with the in-memory
    tracker (which has no TTL), so wiring it unconditionally is safe.
    """

    async def _tick() -> None:
        await refresh_presence(sio, presence, namespace=namespace)

    return _periodic_step(
        tick=_tick, interval=interval, label="presence_heartbeat", step_id=step_id
    )
