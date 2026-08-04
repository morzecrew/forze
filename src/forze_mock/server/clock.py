"""A clock the control plane can freeze and advance."""

from __future__ import annotations

import time
from datetime import UTC, datetime, timedelta
from typing import Any, final
from uuid import UUID

import attrs

from forze.base.exceptions import exc

# ----------------------- #

_RUNNING_HEADROOM = timedelta(days=1)
"""How much room a **running** clock must keep below the end of representable time.

A running clock holds a fixed offset while the wall clock keeps moving, so an instant that is
merely representable at the moment of the call goes out of range on the next read. That
matters more than it sounds: ``now()`` is what every request calls through the bound time
source, so the failure lands on all of them rather than on the call that caused it. A frozen
clock needs no headroom — it does not advance on its own.
"""

# ....................... #


def _refuse_unrunnable(instant: datetime) -> None:
    """Refuse to leave the clock *running* at an instant with no room left to run."""

    try:
        _ = instant + _RUNNING_HEADROOM

    except OverflowError as error:
        raise exc.validation(
            f"A running clock must stay at least {_RUNNING_HEADROOM} below the end of "
            f"representable time, and {instant.isoformat()} does not. Freeze there instead"
        ) from error


# ....................... #


@final
@attrs.define(slots=True)
class ClockMiddleware:
    """Bind the server's one controlled clock into the context of each request.

    Per request, not once at startup: a ``TimeSource`` lives in a ContextVar, and a value
    bound inside the lifespan task is *not* visible to the tasks the server spawns per
    request — they copy the context that existed before it. Binding at startup looks right,
    passes a naive read, and leaves every handler on the system clock. The source itself is
    long-lived and shared, so what the control plane changes, every request sees.
    """

    app: Any
    clock: ControlledTimeSource

    # ....................... #

    async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
        from forze.base.primitives import bind_time_source

        with bind_time_source(self.clock):
            await self.app(scope, receive, send)


# ....................... #


@final
@attrs.define(slots=True)
class ControlledTimeSource:
    """The system clock until someone freezes it, then whatever the control plane says.

    One long-lived instance, bound into each request's context by :class:`ClockMiddleware`.
    The binding has to be per request (a ``TimeSource`` lives in a ContextVar), but the
    *state* is shared, and that is what makes ``POST /_mock/time`` affect the whole server
    rather than only the request that asked for it.
    """

    frozen_at: datetime | None = None
    """When set, every read returns this instant."""

    offset: timedelta = timedelta()
    """Added to the wall clock while not frozen — a running clock, shifted."""

    _counter: int = attrs.field(default=0, init=False)

    # ....................... #

    def now(self) -> datetime:
        if self.frozen_at is not None:
            return self.frozen_at

        return datetime.now(UTC) + self.offset

    # ....................... #

    def uuid(self) -> UUID:
        from forze.base.primitives.uuid import uuid7

        base_ns = int(self.now().timestamp() * 1_000_000_000)

        if self.frozen_at is None:
            return uuid7(timestamp_ns=base_ns)

        # Frozen: the timestamp cannot order ids, so a counter does — otherwise every
        # document created while time is stopped collides on the sortable prefix.
        result = uuid7(timestamp_ns=base_ns + self._counter)
        self._counter += 1

        return result

    # ....................... #

    def monotonic(self) -> float:
        # Only the wall clock is controlled; deadlines and idle timeouts keep elapsing, so a
        # frozen server does not silently stop timing out.
        return time.monotonic()

    # ....................... #

    def freeze(self, instant: datetime | None = None) -> datetime:
        """Stop the clock at *instant* (default: now). Returns the instant it stopped at.

        A naive *instant* is read as UTC rather than as local time. Storing it naive would
        poison every later read: ``now()`` would return a naive datetime, and the first
        comparison against an aware one — a TTL, an expiry — raises instead of answering.

        "Naive" here is Python's own test, ``utcoffset() is not None``, and not the presence
        of a ``tzinfo``: an attached zone that declines to state an offset leaves the datetime
        naive, and ``astimezone`` reads a naive one as **local** time. Getting that wrong
        stops the clock at a different instant on every host — precisely what a deterministic
        clock exists to prevent.

        An aware one is **converted** to UTC rather than kept at its own offset, and refused
        when it has no UTC equivalent. Every later operation normalizes to UTC anyway —
        subtracting two instants, taking a ``timestamp()`` for an id — so an offset-aware
        instant near either end of the range is representable exactly as written and
        overflows the moment anything does arithmetic with it. Normalizing here is what makes
        one stored instant safe for all of them, instead of each one guarding separately.
        """

        if instant is None:
            self.frozen_at = self.now()

            return self.frozen_at

        aware = instant if instant.utcoffset() is not None else instant.replace(tzinfo=UTC)

        try:
            self.frozen_at = aware.astimezone(UTC)

        except (OverflowError, OSError, ValueError) as error:
            raise exc.validation(
                f"{instant.isoformat()} has no representable UTC equivalent, so a clock "
                "stopped there could not be read"
            ) from error

        return self.frozen_at

    # ....................... #

    def advance(self, delta: timedelta) -> datetime:
        """Move the clock forward, frozen or running.

        Forward only. A backwards step would hand out ids whose sortable prefix goes back on
        itself, and it buys nothing: :meth:`freeze` already sets any instant, earlier ones
        included, which is the honest way to ask for an earlier clock.

        The *destination* is what gets checked, not the step: a representable ``timedelta``
        added to a clock already frozen near ``datetime.max`` still lands outside the range,
        and computing it before mutating is what keeps a refused advance from leaving the
        clock in a state whose next read raises. A destination the clock will keep *running*
        from needs more than that — see :data:`_RUNNING_HEADROOM`.
        """

        if delta < timedelta():
            raise exc.configuration(
                f"The controlled clock only advances forward; got {delta}. "
                "Use freeze(instant) to set an earlier moment"
            )

        try:
            moved = self.now() + delta

        except OverflowError as error:
            raise exc.validation(
                f"Advancing by {delta} from {self.now().isoformat()} leaves the range of "
                "representable dates"
            ) from error

        if self.frozen_at is not None:
            self.frozen_at = moved

        else:
            _refuse_unrunnable(moved)
            self.offset += delta

        return self.now()

    # ....................... #

    def resume(self) -> datetime:
        """Let the clock run again, keeping whatever offset it has accumulated.

        Refused, and the clock left frozen, when there is no room to run from where it
        stopped: a frozen instant near ``datetime.max`` is perfectly readable, and becomes
        unreadable the moment the wall clock is allowed to carry it forward.
        """

        if self.frozen_at is not None:
            _refuse_unrunnable(self.frozen_at)
            self.offset = self.frozen_at - datetime.now(UTC)
            self.frozen_at = None

        return self.now()
