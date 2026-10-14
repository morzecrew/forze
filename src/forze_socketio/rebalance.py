"""Shed a replica's movable connections while it holds well above the fleet's mean.

A long-lived socket never moves on its own, so a fleet that drifted out of balance — the
replica that restarted last holds few connections, the others the rest — can stay that way
for hours. The rebalance step closes a few of an overloaded replica's movable connections
at a time, and their clients reconnect through the balancer, which must place them on a
less-loaded replica: route WebSocket traffic least-connections.

Everything is counted in Engine.IO connections — transports — because that is what a close
ends: one transport carries every namespace its client opened.
"""

from ._compat import require_socketio

require_socketio()

# ....................... #

import asyncio
import math
from collections.abc import Callable, Sequence
from datetime import timedelta
from typing import final

import attrs
from socketio.async_server import AsyncServer

from forze.application.contracts.execution import LifecycleStep
from forze.application.contracts.fleet import FleetPort, FleetSpec, Report, validate_member_id
from forze.application.execution import ExecutionContext
from forze.application.execution.background import (
    DEFAULT_STOP_GRACE_SECONDS,
    periodic_lifecycle_step,
)
from forze.application.execution.context.lifecycle import scope_readiness
from forze.base.exceptions import exc
from forze.base.primitives import StrKey, instance_id, monotonic

from .drain import ConnectionSelector, _closable, _transports, close_spread
from .exceptions import error_logger

# ----------------------- #

__all__ = [
    "connection_rebalance_lifecycle_step",
    "connection_report",
]


def _live_counts(
    sio: AsyncServer,
    *,
    namespaces: Sequence[str],
    movable: ConnectionSelector,
) -> tuple[int, list[tuple[str, str]]]:
    """This node's transports carrying any of *namespaces*, and the movable ones among them.

    A transport is movable when every namespace it carries is in *namespaces* and accepted
    by *movable* — the drain's own rule — so one game namespace keeps the whole transport.
    """

    transports = _transports(sio)
    load = sum(
        1
        for members in transports.values()
        if any(namespace in namespaces for namespace, _sid in members)
    )

    return load, _closable(transports, namespaces=namespaces, select=movable)


# ....................... #


def connection_report(
    sio: AsyncServer,
    *,
    movable: ConnectionSelector,
    namespaces: Sequence[str] = ("/",),
    load: str = "connections",
    movable_report: str = "movable",
) -> Callable[[], Report]:
    """The fleet membership report the rebalance step reads from its peers.

    Pass it as ``fleet_membership_lifecycle_step(report=...)``. Each call counts this node's
    transports the way :func:`connection_rebalance_lifecycle_step` counts its own, so give
    both the same *namespaces*, *movable*, *load* and *movable_report*.

    :param movable: ``(namespace, sid) -> bool``: whether that connection may be moved.
    :param namespaces: The namespaces counted.
    :param load: The report key for the transports carrying any of *namespaces*.
    :param movable_report: The report key for the movable transports among them.
    :returns: A callable returning ``{load: ..., movable_report: ...}``.
    """

    covered = tuple(namespaces)

    def report() -> Report:
        count, targets = _live_counts(sio, namespaces=covered, movable=movable)

        return {load: count, movable_report: len(targets)}

    return report


# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class _Rebalance:
    """One replica's shedding: its rate allowance, its last shed, and the shed running now."""

    sio: AsyncServer
    spec: FleetSpec
    every: timedelta
    member_id: str | None
    namespaces: tuple[str, ...]
    load: str
    movable_report: str
    movable: ConnectionSelector
    per_minute: int
    tolerance: float
    settle: timedelta
    cooldown: timedelta

    ctx: ExecutionContext | None = attrs.field(default=None, init=False)
    port: FleetPort | None = attrs.field(default=None, init=False)
    me: str = attrs.field(default="", init=False)

    credit: float = attrs.field(default=0.0, init=False)
    """The allowance, in sixtieths of a connection: it grows by ``per_minute`` a second, so
    whole-second intervals add exact whole numbers."""

    ticked_at: float | None = attrs.field(default=None, init=False)
    shed_ended_at: float | None = attrs.field(default=None, init=False)
    shed: asyncio.Task[None] | None = attrs.field(default=None, init=False)

    # ....................... #

    @property
    def loop_name(self) -> str:
        """Satisfies ``DrainableLoop``: the runtime stops a running shed before teardown."""

        return f"realtime_connection_shed:{self.spec.name}"

    # ....................... #

    def bind(self, ctx: ExecutionContext) -> None:
        """Take this scope's fleet port and the member id, and start afresh, at every startup."""

        self.ctx = ctx
        self.port = ctx.fleet(self.spec)
        # Resolved here, as the membership step does, so both name the same member.
        self.me = self.member_id if self.member_id is not None else instance_id()
        # A new scope starts with an empty allowance and no cooldown, as the first one did.
        self.credit = 0.0
        self.ticked_at = self.shed_ended_at = None

    # ....................... #

    def _bound(self) -> tuple[ExecutionContext, FleetPort]:
        if self.ctx is None or self.port is None:
            raise exc.internal("Connection rebalance ticks before its startup bound a scope")

        return self.ctx, self.port

    # ....................... #

    def _refill(self, now: float) -> None:
        # Real elapsed time, not ``every``: the loop sleeps after a tick ends, and a stalled
        # loop must not be paid for ticks it never ran.
        if self.ticked_at is not None:
            self.credit = min(
                self.per_minute * 60.0,
                self.credit + self.per_minute * (now - self.ticked_at),
            )

        self.ticked_at = now

    # ....................... #

    def _quiet(self, ctx: ExecutionContext, now: float) -> bool:
        """Whether this tick must not shed, before anything is read."""

        if scope_readiness(ctx) != "ready":
            # Not serving yet, or draining: a draining replica closes everything in its drain.
            return True

        if self.shed is not None and not self.shed.done():
            return True

        return (
            self.shed_ended_at is not None
            and now - self.shed_ended_at < self.cooldown.total_seconds()
        )

    # ....................... #

    async def tick(self) -> None:
        """Refill the allowance; then, unless quiet, shed this replica's excess."""

        ctx, port = self._bound()
        now = monotonic()
        self._refill(now)

        if self._quiet(ctx, now):
            return

        view = await port.view()
        mine = next((member for member in view.members if member.id == self.me), None)

        # A replica that reports no load is outside every peer's mean, so it sheds nothing.
        if mine is None or self.load not in mine.report:
            return

        # Again after the read: a drain announced meanwhile closes the connections itself, and
        # the runtime may already have stopped this step's shed, so a new one would run on.
        if scope_readiness(ctx) != "ready":
            return

        load, targets = _live_counts(self.sio, namespaces=self.namespaces, movable=self.movable)
        # Live counts, not this replica's own report, which can be a beat behind its last shed.
        live = attrs.evolve(
            mine,
            report={**mine.report, self.load: load, self.movable_report: len(targets)},
        )
        view = attrs.evolve(
            view,
            members=[live if member.id == self.me else member for member in view.members],
        )
        excess = view.excess(
            self.me,
            load=self.load,
            movable=self.movable_report,
            tolerance=self.tolerance,
            settle=self.settle,
        )
        count = min(excess, math.floor(self.credit / 60.0))

        if count < 1:
            return

        self.credit -= count * 60.0

        error_logger.info(
            "Rebalance: shedding %d connection(s) of an excess of %d (%d open, %d movable)",
            count,
            excess,
            load,
            len(targets),
        )

        self.shed = asyncio.create_task(
            self._shed(targets[:count]),
            name=self.loop_name,
        )

    # ....................... #

    async def _shed(self, targets: list[tuple[str, str]]) -> None:
        try:
            await close_spread(self.sio, targets, self.every, tail=self.every)

        finally:
            # The cooldown counts from here: the last close's client has only now left.
            self.shed_ended_at = monotonic()

    # ....................... #

    async def stop(self, *, deadline: float) -> bool:
        """Cancel a shed still running. Idempotent.

        :returns: ``True`` when no shed was running.
        """

        shed = self.shed

        if shed is None or shed.done():
            return True

        shed.cancel()
        clock = asyncio.get_running_loop()
        await asyncio.wait([shed], timeout=max(0.0, deadline - clock.time()))

        return False


# ----------------------- #


def connection_rebalance_lifecycle_step(
    sio: AsyncServer,
    *,
    fleet: FleetSpec,
    every: timedelta,
    movable: ConnectionSelector,
    per_minute: int,
    member_id: str | None = None,
    namespaces: Sequence[str] = ("/",),
    load: str = "connections",
    movable_report: str = "movable",
    tolerance: float = 0.15,
    settle: timedelta = timedelta(seconds=120),
    cooldown: timedelta | None = None,
    step_id: StrKey = "realtime_connection_rebalance",
) -> LifecycleStep:
    """A lifecycle step that sheds this replica's movable connections while it is overloaded.

    Every *every* the step refills a rate allowance of *per_minute* connections a minute
    (fractions carried, never more than *per_minute* banked), then sheds nothing while the
    scope is not ready (starting or draining), while an earlier shed is still closing, or
    within *cooldown* of the last shed's end. Otherwise it reads the fleet's view, puts this
    replica's live counts in its own entry, and asks
    :meth:`~forze.application.contracts.fleet.FleetView.excess` how far above the mean it
    is. It closes that many movable transports, at most the allowance, with
    :func:`~forze_socketio.close_spread` over *every*, each given up to another *every* to
    finish. Their clients reconnect through the balancer, which must route
    least-connections so they land on a replica with room.

    Counts are Engine.IO transports. A transport is movable when every namespace it carries
    is in *namespaces* and accepted by *movable*. Peers' counts come from their membership
    reports under *load* and *movable_report*, in the same unit:
    :func:`connection_report` fills them. A replica that reports no *load* is left out of
    the mean and sheds nothing itself, and so does one with no entry in the view or not
    serving. This replica's own reported values are never used: its live counts replace them.

    :param fleet: The fleet the membership step beats into.
    :param every: How often to tick.
    :param movable: ``(namespace, sid) -> bool``: whether that connection may be moved. One
        that raises keeps that transport, logged.
    :param per_minute: The most connections shed in a minute.
    :param member_id: This replica's id in the fleet; the membership step's, which is
        :func:`~forze.base.primitives.instance_id` when ``None``.
    :param namespaces: The namespaces counted and shed.
    :param load: The report key holding each replica's transport count.
    :param movable_report: The report key holding each replica's movable transport count.
    :param tolerance: How far above the mean, relative to it, is still balanced.
    :param settle: How long every replica must have been serving before anyone sheds.
    :param cooldown: The least time between one shed's end and the next shed; *settle* when
        ``None``.
    :param step_id: The step's id.
    :returns: The lifecycle step.
    :raises CoreException: ``configuration`` for a non-positive *every*, a *per_minute*
        below 1, a negative or non-finite *tolerance*, a negative *settle* or *cooldown*, or
        no *namespaces*; ``validation`` for an invalid *member_id*.
    """

    covered = tuple(namespaces)

    if per_minute < 1:
        raise exc.configuration(f"Connection rebalance sheds at least 1 a minute, not {per_minute}")

    if not math.isfinite(tolerance) or tolerance < 0:
        raise exc.configuration(
            f"Connection rebalance tolerance must be 0 or more, not {tolerance}"
        )

    if settle < timedelta(0) or (cooldown is not None and cooldown < timedelta(0)):
        raise exc.configuration("Connection rebalance settle and cooldown must not be negative")

    if not covered:
        raise exc.configuration("Connection rebalance needs at least one namespace")

    if member_id is not None:
        validate_member_id(member_id)

    rebalance = _Rebalance(
        sio=sio,
        spec=fleet,
        every=every,
        member_id=member_id,
        namespaces=covered,
        load=load,
        movable_report=movable_report,
        movable=movable,
        per_minute=per_minute,
        tolerance=tolerance,
        settle=settle,
        cooldown=settle if cooldown is None else cooldown,
    )
    loop = periodic_lifecycle_step(
        tick=rebalance.tick,
        interval=every,
        name=f"realtime_connection_rebalance:{fleet.name}",
        step_id=step_id,
    )

    async def startup(ctx: ExecutionContext) -> None:
        rebalance.bind(ctx)
        ctx.drainables.register(rebalance)
        await loop.startup(ctx)

    async def shutdown(ctx: ExecutionContext) -> None:
        # Normally both already stopped: the runtime stops every drainable before teardown.
        await loop.shutdown(ctx)
        clock = asyncio.get_running_loop()
        await rebalance.stop(deadline=clock.time() + DEFAULT_STOP_GRACE_SECONDS)

    return LifecycleStep(
        id=step_id,
        startup=startup,
        shutdown=shutdown,
        requires_long_running=True,
    )
