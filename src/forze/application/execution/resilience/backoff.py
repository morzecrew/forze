"""Backoff delay computation with jitter modes."""

import math
import random
from typing import Final

from forze.application.contracts.resilience import BackoffStrategy
from forze.base.exceptions import CoreException, ExceptionKind

# ----------------------- #

RETRY_HINT_KINDS: Final = frozenset({ExceptionKind.THROTTLED, ExceptionKind.UNAVAILABLE})
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


def retry_after(error: CoreException) -> float | None:
    """Return the soonest a retry of a throttled or unavailable *error* can succeed, in seconds.

    Reads ``details["retry_after"]`` — the upstream's ``Retry-After``, as the HTTP client
    maps a ``429`` — and returns ``None`` for any other kind, a missing value, or one that is
    not a non-negative number. An infinite hint, or one too large for a float, is
    ``math.inf``: no retry waits it out.
    """

    if error.kind not in RETRY_HINT_KINDS or not error.details:
        return None

    value = error.details.get("retry_after")

    if isinstance(value, bool) or not isinstance(value, int | float):
        return None

    try:
        seconds = float(value)

    except OverflowError:
        seconds = math.inf

    return None if math.isnan(seconds) or seconds < 0 else seconds
