"""Ambient, context-scoped source of wall-clock time and time-ordered ids.

``utcnow()`` and the no-argument ``uuid7()`` read the active :class:`TimeSource` rather
than the system clock directly, so a scope can make every time/id read deterministic
(tests) or replay-stable (durable workflows) **without changing call sites** — domain
code keeps calling ``utcnow()`` / ``uuid7()`` and stays clock-free in its own source.

The default source is the system clock, so nothing changes unless a source is bound.
"""

from __future__ import annotations

import time
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import UTC, datetime
from typing import Protocol, final, runtime_checkable
from uuid import UUID

import attrs

# ----------------------- #


@runtime_checkable
class TimeSource(Protocol):
    """A source of the current time and a fresh time-ordered id."""

    def now(self) -> datetime:
        """Return the current timezone-aware UTC datetime."""
        ...  # pragma: no cover

    def uuid(self) -> UUID:
        """Return a fresh time-ordered (UUIDv7-style) id for 'now'."""
        ...  # pragma: no cover

    def monotonic(self) -> float:
        """Return a monotonic clock reading in fractional seconds.

        For relative timing (deadlines, backoff, TTLs) — a non-decreasing value with
        an arbitrary epoch, never wall-clock. A simulation source ties this to the
        virtual event-loop clock so timed work is deterministic.
        """
        ...  # pragma: no cover


# ....................... #


@final
@attrs.define(slots=True, frozen=True)
class SystemTimeSource:
    """The real wall clock — the default source (identical to direct stdlib reads)."""

    def now(self) -> datetime:
        return datetime.now(UTC)

    # ....................... #

    def uuid(self) -> UUID:
        from .uuid import uuid7  # lazy: avoids the uuid <-> time_source import cycle

        return uuid7(timestamp_ns=time.time_ns())

    # ....................... #

    def monotonic(self) -> float:
        return time.monotonic()


# ....................... #


@final
@attrs.define(slots=True)
class FrozenTimeSource:
    """A fixed clock for tests: a constant ``now`` and deterministic, ordered ids."""

    instant: datetime

    # ....................... #

    _counter: int = attrs.field(default=0, init=False)

    # ....................... #

    def now(self) -> datetime:
        return self.instant

    # ....................... #

    def uuid(self) -> UUID:
        from .uuid import uuid7

        base_ns = int(self.instant.timestamp() * 1_000_000_000)
        result = uuid7(timestamp_ns=base_ns + self._counter)
        self._counter += 1

        return result

    # ....................... #

    def monotonic(self) -> float:
        # Only the *wall* clock is frozen (deterministic timestamps/ids); relative
        # timing stays real, so deadlines/idle-timeouts still elapse under a frozen
        # source. Virtual, deterministic relative time is the simulation source's job.
        return time.monotonic()


# ....................... #

_TIME_SOURCE: ContextVar[TimeSource] = ContextVar(
    "time_source",
    default=SystemTimeSource(),
)


def current_time_source() -> TimeSource:
    """Return the time source active in the current context."""

    return _TIME_SOURCE.get()


@contextmanager
def bind_time_source(source: TimeSource) -> Iterator[None]:
    """Bind *source* as the active time source for the duration of the block."""

    token = _TIME_SOURCE.set(source)

    try:
        yield

    finally:
        _TIME_SOURCE.reset(token)
