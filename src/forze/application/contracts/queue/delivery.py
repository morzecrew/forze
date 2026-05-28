"""Shared helpers for queue delivery timing."""

from datetime import datetime, timedelta, timezone
from typing import Callable

from forze.base.exceptions import exc
from forze.base.primitives import utcnow

# ----------------------- #

SQS_MAX_DELAY = timedelta(seconds=900)
"""Maximum delay supported by Amazon SQS ``DelaySeconds``."""

# ....................... #


def resolve_delivery_delay(
    *,
    delay: timedelta | None,
    not_before: datetime | None,
    now: Callable[[], datetime] = utcnow,
) -> timedelta | None:
    """Return a non-negative delay until the message may be received, or ``None`` for immediate delivery.

    :param delay: Relative delay from *now*.
    :param not_before: Absolute UTC instant when the message may be received.
    :raises exc.precondition: When both *delay* and *not_before* are set, *delay* is negative,
        or *not_before* is naive.
    """

    if delay is not None and not_before is not None:
        raise exc.precondition(
            "queue enqueue: delay and not_before are mutually exclusive"
        )

    if delay is not None:
        if delay < timedelta(0):
            raise exc.precondition("queue enqueue: delay must be non-negative")

        return delay if delay > timedelta(0) else None

    if not_before is None:
        return None

    if not_before.tzinfo is None:
        raise exc.precondition("queue enqueue: not_before must be timezone-aware")

    instant = not_before.astimezone(timezone.utc)
    resolved = instant - now().astimezone(timezone.utc)

    if resolved <= timedelta(0):
        return None

    return resolved
