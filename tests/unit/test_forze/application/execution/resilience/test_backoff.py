"""Tests for backoff delay computation and jitter modes."""

from __future__ import annotations

import random
from datetime import timedelta

from forze.application.contracts.resilience import BackoffStrategy
from forze.application.execution.resilience.backoff import compute_delay

# ----------------------- #


def _backoff(jitter: str) -> BackoffStrategy:
    return BackoffStrategy(
        base=timedelta(seconds=1),
        max=timedelta(seconds=10),
        multiplier=2.0,
        jitter=jitter,  # type: ignore[arg-type]
    )


# ....................... #


def test_none_is_exact_exponential() -> None:
    rng = random.Random(0)
    assert compute_delay(_backoff("none"), 1, 0.0, rng) == 1.0
    assert compute_delay(_backoff("none"), 2, 0.0, rng) == 2.0
    assert compute_delay(_backoff("none"), 3, 0.0, rng) == 4.0


def test_none_respects_cap() -> None:
    rng = random.Random(0)
    # 1 * 2**5 = 32, capped at max=10
    assert compute_delay(_backoff("none"), 6, 0.0, rng) == 10.0


def test_full_jitter_within_bounds() -> None:
    rng = random.Random(1)
    for _ in range(50):
        delay = compute_delay(_backoff("full"), 2, 0.0, rng)
        assert 0.0 <= delay <= 2.0


def test_equal_jitter_within_bounds() -> None:
    rng = random.Random(2)
    for _ in range(50):
        delay = compute_delay(_backoff("equal"), 2, 0.0, rng)
        assert 1.0 <= delay <= 2.0


def test_decorrelated_within_bounds() -> None:
    rng = random.Random(3)
    prev = 0.0
    for _ in range(50):
        delay = compute_delay(_backoff("decorrelated"), 1, prev, rng)
        assert 1.0 <= delay <= 10.0
        prev = delay
