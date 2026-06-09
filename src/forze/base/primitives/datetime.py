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
