"""Backoff delay computation with jitter modes."""

import random

from forze.application.contracts.resilience import BackoffStrategy

# ----------------------- #


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
    raw = min(cap, base * (strategy.multiplier ** (attempt - 1)))

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
