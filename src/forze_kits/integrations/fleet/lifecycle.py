"""Keep this replica's fleet entry current for as long as it runs.

The step's state follows the scope it runs in, through the hook's ``ctx``: ``starting``
until lifecycle startup finished, ``serving`` after, and ``draining`` once a drain is
announced or the drain gate closes. It never needs a handle on the runtime.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import timedelta
from typing import TYPE_CHECKING, Final, Literal, final

import attrs

from forze.application.contracts.execution import LifecycleHook, LifecycleStep
from forze.application.contracts.fleet import (
    FleetPort,
    FleetSpec,
    MemberState,
    Report,
    validate_member_id,
)
from forze.application.execution.background import periodic_lifecycle_step
from forze.application.execution.context.lifecycle import Readiness, scope_readiness
from forze.base.exceptions import exc
from forze.base.primitives import StrKey, instance_id

if TYPE_CHECKING:
    from forze.application.execution import ExecutionContext

# ----------------------- #

_STATE: Final[dict[Readiness, MemberState]] = {
    "unavailable": "starting",
    "ready": "serving",
    "draining": "draining",
}
"""The member state each scope readiness beats as."""

# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class _Membership:
    """This replica's entry: what it beats, and the scope it beats for."""

    spec: FleetSpec
    report: Callable[[], Report]
    member_id: str | None

    ctx: ExecutionContext | None = attrs.field(default=None, init=False)
    port: FleetPort | None = attrs.field(default=None, init=False)
    me: str = attrs.field(default="", init=False)

    # ....................... #

    def bind(self, ctx: ExecutionContext) -> None:
        """Take this scope's port and the member id, at every startup."""

        self.ctx = ctx
        self.port = ctx.fleet(self.spec)
        # Resolved here, not at construction: a step built before a fork beats as the child.
        self.me = self.member_id if self.member_id is not None else instance_id()

    # ....................... #

    def _bound(self) -> tuple[ExecutionContext, FleetPort]:
        if self.ctx is None or self.port is None:
            raise exc.internal("Fleet membership beats before its startup bound a scope")

        return self.ctx, self.port

    # ....................... #

    async def beat(self) -> None:
        """Beat in the state the scope is in."""

        ctx, port = self._bound()

        await port.beat(self.me, _STATE[scope_readiness(ctx)], self.report())

    # ....................... #

    async def stop(self, on_stop: Literal["leave", "linger"], linger_for: timedelta) -> None:
        """Leave the fleet at once, or beat one last ``draining`` entry that outlives us."""

        _, port = self._bound()

        if on_stop == "leave":
            await port.leave(self.me)

        else:
            await port.beat(self.me, "draining", self.report(), ttl=linger_for)


# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class _FleetStartup(LifecycleHook):
    """Beat once as ``starting``, then start the loop that keeps beating."""

    membership: _Membership
    loop: LifecycleHook

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        self.membership.bind(ctx)
        # Awaited, so an unwired port or a report the codec refuses fails the boot once
        # rather than logging on every beat.
        await self.membership.beat()
        await self.loop(ctx)


# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class _FleetShutdown(LifecycleHook):
    """Stop the loop, then leave or linger."""

    membership: _Membership
    loop: LifecycleHook
    on_stop: Literal["leave", "linger"]
    linger_for: timedelta

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        # Normally already stopped: the runtime stops every drainable before teardown.
        await self.loop(ctx)
        await self.membership.stop(self.on_stop, self.linger_for)


# ----------------------- #


def fleet_membership_lifecycle_step(
    spec: FleetSpec,
    *,
    every: timedelta,
    report: Callable[[], Report] = dict,
    on_stop: Literal["leave", "linger"] = "leave",
    linger_for: timedelta = timedelta(0),
    member_id: str | None = None,
    depends_on: tuple[StrKey, ...] = (),
    step_id: StrKey = "fleet_membership",
) -> LifecycleStep:
    """Keep this replica's entry in the fleet *spec* names, from startup to shutdown.

    At startup the step beats once as ``starting``, then beats every *every* from a supervised
    loop: a failed beat is logged and the next one tries again. The state follows the scope:
    ``starting`` until lifecycle startup finished, ``serving`` after, ``draining`` once a
    drain is announced or the drain gate closes. On stop it leaves the fleet, or lingers.

    Every replica runs it, so it is not marked as mutating shared state: it writes only its
    own entry and removes expired ones, which is safe from every replica at once.

    :param spec: The fleet.
    :param every: How often to beat; below half of ``spec.ttl``, so one missed beat does
        not drop the member.
    :param report: Called on every beat for the report to publish.
    :param on_stop: ``leave`` removes the entry at once, so peers re-rank immediately;
        ``linger`` writes one last ``draining`` entry that lives for *linger_for*, so peers
        see the deploy in progress without this process waiting for it.
    :param linger_for: How long the lingering entry lives.
    :param member_id: The id to beat as; :func:`~forze.base.primitives.instance_id` when
        ``None``.
    :param depends_on: Steps that must shut down after this one — the step that closes the
        fleet backend's client, since stopping writes to it.
    :param step_id: The step's id.
    :returns: The lifecycle step.
    :raises CoreException: ``configuration`` for an *every* of half the ttl or more, an
        *on_stop* other than ``leave`` or ``linger``, or a linger with no *linger_for*;
        ``validation`` for an invalid *member_id*.
    """

    if every * 2 >= spec.ttl:
        raise exc.configuration(
            f"Fleet membership beats every {every}, which must be below half of the "
            f"fleet's ttl ({spec.ttl}) so one missed beat does not drop the member"
        )

    if on_stop not in ("leave", "linger"):
        # Refused here, or a typo surfaces only at shutdown, as a failed linger beat.
        raise exc.configuration(f"Fleet membership on_stop is 'leave' or 'linger', not {on_stop!r}")

    if on_stop == "linger" and linger_for < timedelta(milliseconds=1):
        raise exc.configuration("Fleet membership lingers for at least 1 millisecond")

    if member_id is not None:
        validate_member_id(member_id)

    membership = _Membership(spec=spec, report=report, member_id=member_id)
    loop = periodic_lifecycle_step(
        tick=membership.beat,
        interval=every,
        name=f"fleet_membership:{spec.name}",
        step_id=step_id,
    )

    return LifecycleStep(
        id=step_id,
        startup=_FleetStartup(membership=membership, loop=loop.startup),
        shutdown=_FleetShutdown(
            membership=membership,
            loop=loop.shutdown,
            on_stop=on_stop,
            linger_for=linger_for,
        ),
        depends_on=depends_on,
        requires_long_running=True,
    )
