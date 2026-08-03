"""A clock the control plane can freeze and advance."""

from __future__ import annotations

import time
from datetime import UTC, datetime, timedelta
from typing import Any, final
from uuid import UUID

import attrs

# ----------------------- #


@final
@attrs.define(slots=True)
class ClockMiddleware:
    """Bind the controlled clock for the duration of each request.

    Per request, not once at startup: a ``TimeSource`` lives in a ContextVar, and a value
    bound inside the lifespan task is *not* visible to the tasks the server spawns per
    request — they copy the context that existed before it. Binding at startup looks right,
    passes a naive read, and leaves every handler on the system clock.
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

    Bound once for the server's lifetime rather than per request: a ``TimeSource`` lives in a
    ContextVar, and a value set inside one request's task would not be visible to the next.
    Mutating one long-lived source is what makes ``POST /_mock/time`` affect the whole server.
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
        """Stop the clock at *instant* (default: now). Returns the instant it stopped at."""

        self.frozen_at = instant or self.now()

        return self.frozen_at

    # ....................... #

    def advance(self, delta: timedelta) -> datetime:
        """Move the clock forward, frozen or running."""

        if self.frozen_at is not None:
            self.frozen_at += delta

        else:
            self.offset += delta

        return self.now()

    # ....................... #

    def resume(self) -> datetime:
        """Let the clock run again, keeping whatever offset it has accumulated."""

        if self.frozen_at is not None:
            self.offset = self.frozen_at - datetime.now(UTC)
            self.frozen_at = None

        return self.now()
