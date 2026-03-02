"""Datetime primitives shared across the application."""

from datetime import UTC, datetime

# ----------------------- #


def utcnow() -> datetime:
    """Return the current timezone-aware UTC datetime.

    Uses :class:`datetime.datetime` with ``tzinfo=UTC`` for consistent
    timestamp handling across the application.
    """
    return datetime.now(UTC)
