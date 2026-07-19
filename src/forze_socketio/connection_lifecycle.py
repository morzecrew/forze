"""Periodic connection-hygiene steps — credential-expiry sweep and presence heartbeat.

Both run on the shared background-loop machinery
(:class:`~forze.application.execution.background.BackgroundLoopControl`): one bad tick can't
kill the loop, a stop request is honored between ticks (interruptible sleep — shutdown never
waits out an interval), and each loop registers in ``ctx.drainables`` so the runtime stops it
cleanly before teardown. Each node only sees its own connections, so each node runs its own
loops.
"""

from ._compat import require_socketio

require_socketio()

# ....................... #

import asyncio
from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import final

import attrs
from socketio.async_server import AsyncServer

from forze.application.contracts.execution import LifecycleHook, LifecycleStep
from forze.application.execution import ExecutionContext
from forze.application.execution.background import (
    DEFAULT_STOP_GRACE_SECONDS,
    BackgroundLoopControl,
)
from forze.base.exceptions import exc
from forze.base.logging import Logger
from forze.base.primitives import StrKey

from ._logging import ForzeSocketIOLogger
from .connection import RealtimePresence, refresh_presence, sweep_expired_connections
from .observability import BackplaneHealth

# ----------------------- #

_logger = Logger(ForzeSocketIOLogger.ERRORS)


@final
@attrs.define(slots=True, kw_only=True)
class _PeriodicStartup(LifecycleHook):
    """Run *tick* every *interval* until stopped; one bad tick can't kill the loop."""

    tick: Callable[[], Awaitable[None]]
    interval: timedelta
    label: str

    control: BackgroundLoopControl = attrs.field(
        default=attrs.Factory(
            lambda self: BackgroundLoopControl(name=f"realtime_{self.label}"),
            takes_self=True,
        ),
        init=False,
    )
    """Stop signal and bounded teardown, shared with every other background loop."""

    # ....................... #

    @property
    def task(self) -> asyncio.Task[None] | None:
        """The running loop, if any."""

        return self.control.task

    # ....................... #

    @property
    def loop_name(self) -> str:
        """Satisfies ``DrainableLoop``."""

        return self.control.loop_name

    # ....................... #

    async def stop(self, *, deadline: float) -> bool:
        """Stop the loop between ticks. Idempotent — a tick is short and idempotent too."""

        return await self.control.stop(deadline=deadline)

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        if self.control.running:
            return

        self.control.arm()
        self.control.task = asyncio.create_task(self._loop(), name=self.control.loop_name)
        ctx.drainables.register(self)

    # ....................... #

    async def _loop(self) -> None:
        delay = self.interval.total_seconds()

        while True:
            try:
                await self.tick()

            except asyncio.CancelledError:
                raise

            except Exception:
                _logger.critical_exception(f"Realtime {self.label} tick failed")

            if await self.control.sleep_or_stop(delay):
                return


# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class _PeriodicShutdown(LifecycleHook):
    """Stop the periodic loop.

    Normally a no-op — the runtime stops every registered loop before teardown begins. This
    is the fallback for a hand-driven lifecycle; ``stop`` is idempotent.
    """

    startup: _PeriodicStartup

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        clock = asyncio.get_running_loop()
        await self.startup.stop(deadline=clock.time() + DEFAULT_STOP_GRACE_SECONDS)


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

    return _periodic_step(tick=_tick, interval=interval, label="identity_expiry", step_id=step_id)


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


# ....................... #


def realtime_backplane_heartbeat_lifecycle_step(
    sio: AsyncServer,
    health: BackplaneHealth,
    *,
    namespace: str = "/",
    probe_room: str = "forze:backplane:probe",
    interval: timedelta = timedelta(seconds=15),
    step_id: StrKey = "realtime_backplane_heartbeat",
) -> LifecycleStep:
    """Periodically push a probe frame through the Socket.IO manager and record freshness.

    A multi-process deployment's every cross-node emit rides the ``AsyncRedisManager``
    backplane, whose listener can die without anything in this process noticing — emits
    keep "succeeding" into a void. The probe emits to a room nobody joins: with a Redis
    manager that is a real publish through the backplane (failure = Redis/manager down),
    with the default single-process manager it is a no-op success (nothing to monitor).
    The manager's listener task is checked too, when it exposes one — the delivery leg
    can die while publishes keep succeeding, and that is equally a dead backplane.
    Feed *health* to ``instrument_realtime_backplane`` and alarm on staleness.
    """

    async def _tick() -> None:
        try:
            await sio.emit("forze.backplane.probe", data={}, room=probe_room, namespace=namespace)

            # The emit only proves the *publish* leg. The other half of a dead backplane
            # is this node's listener — the task delivering other nodes' emits INTO this
            # process — which can die while publishes keep "succeeding". python-socketio's
            # pub/sub managers hold it as ``manager.thread``; when present and finished,
            # the backplane is down for delivery no matter what the probe publish said.
            listener = getattr(sio.manager, "thread", None)
            done = getattr(listener, "done", None)

            if listener is not None and callable(done) and done():
                raise RuntimeError(
                    "Socket.IO backplane listener task has exited — cross-node emits "
                    "are no longer delivered to this node"
                )

        except Exception:
            health.failed()
            raise  # the periodic loop logs the failed tick and keeps going

        health.ok()

    return _periodic_step(
        tick=_tick, interval=interval, label="backplane_heartbeat", step_id=step_id
    )
