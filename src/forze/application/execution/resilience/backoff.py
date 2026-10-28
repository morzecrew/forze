"""Backoff delay computation with jitter modes."""

import math
import random
from datetime import UTC, datetime
from typing import Final

from forze.application.contracts.resilience import BackoffStrategy
from forze.base.exceptions import CoreException, ExceptionKind
from forze.base.primitives import utcnow

# ----------------------- #

_RETRY_HINT_KINDS: Final = frozenset({ExceptionKind.THROTTLED, ExceptionKind.UNAVAILABLE})
"""The kinds whose ``retry_after`` / ``retry_at`` details say when a retry can succeed."""

# ....................... #


def compute_delay(
    strategy: BackoffStrategy,
    attempt: int,
    prev_delay: float,
    rng: random.Random,
) -> float:
    """Return the delay (seconds) before retry ``attempt`` (1-based).

    ``prev_delay`` is the previous computed delay, used by decorrelated jitter.
    """

    base = strategy.base.total_seconds()
    cap = strategy.max.total_seconds()

    try:
        raw = min(cap, base * (float(strategy.multiplier) ** (attempt - 1)))

    except OverflowError:
        # A float power past ~1e308 raises instead of saturating; the cap still holds.
        raw = cap

    if strategy.jitter == "none":
        return raw

    if strategy.jitter == "full":
        return rng.uniform(0.0, raw)

    if strategy.jitter == "equal":
        half = raw / 2
        return half + rng.uniform(0.0, half)

    # decorrelated
    prev = prev_delay if prev_delay > 0 else base

    return min(cap, rng.uniform(base, prev * 3))


# ....................... #


def _hint_seconds(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None

    try:
        seconds = float(value)

    except OverflowError:
        seconds = math.inf

    return None if math.isnan(seconds) or seconds < 0 else seconds


# ....................... #


def retry_hint(error: CoreException) -> float | datetime | None:
    """Return when a throttled or unavailable *error* says a retry can succeed, as it says it.

    A finite, non-negative ``details["retry_after"]`` — seconds, the upstream's
    ``Retry-After`` as the HTTP client maps a ``429`` — comes first; otherwise
    ``details["retry_at"]``, an aware :class:`~datetime.datetime`, in UTC; otherwise an
    infinite ``retry_after`` (or one too large for a float) as ``math.inf``: no retry waits
    it out. Any other kind, or no usable value — a negative or NaN number, a string, a
    naive datetime, an instant UTC cannot represent — is ``None``.
    """

    if error.kind not in _RETRY_HINT_KINDS or not error.details:
        return None

    seconds = _hint_seconds(error.details.get("retry_after"))

    if seconds is not None and math.isfinite(seconds):
        return seconds

    at = error.details.get("retry_at")

    if isinstance(at, datetime) and at.utcoffset() is not None:
        try:
            return at.astimezone(UTC)

        except (OverflowError, ValueError):  # an instant UTC cannot represent
            pass

    return seconds


# ....................... #


def retry_after(error: CoreException) -> float | None:
    """Return the soonest a retry of a throttled or unavailable *error* can succeed, in seconds.

    Reads :func:`retry_hint`; a ``retry_at`` instant becomes the seconds until it, ``0`` once
    it has passed. ``math.inf`` means no retry waits it out, ``None`` that there is no hint.
    """

    hint = retry_hint(error)

    if isinstance(hint, datetime):
        return max(0.0, (hint - utcnow()).total_seconds())

    return hint
