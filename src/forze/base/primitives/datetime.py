"""Datetime primitives shared across the application."""

from datetime import UTC, datetime

# ----------------------- #


def utcnow() -> datetime:
    """Return the current timezone-aware UTC datetime."""

    return datetime.now(UTC)
