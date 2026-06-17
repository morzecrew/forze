"""Datetime primitives shared across the application."""

from datetime import datetime

from .time_source import current_time_source

# ----------------------- #


def utcnow() -> datetime:
    """Return the current timezone-aware UTC datetime.

    Reads the context-active :class:`~forze.base.primitives.time_source.TimeSource`
    (the system clock by default), so a bound source — a frozen clock in tests or a
    durable workflow's deterministic clock — controls every ``utcnow()`` read.
    """

    return current_time_source().now()


def monotonic() -> float:
    """Return a monotonic clock reading (fractional seconds), via the active TimeSource.

    The relative-timing twin of :func:`utcnow` — for deadlines, backoff, and TTLs.
    Defaults to ``time.monotonic()``; a bound simulation source ties it to the virtual
    event-loop clock so timed work is deterministic. Never wall-clock.
    """

    return current_time_source().monotonic()
