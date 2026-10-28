"""Tests for backoff delay computation and jitter modes."""

from __future__ import annotations

import math
import random
from datetime import timedelta

import pytest

from forze.application.contracts.resilience import BackoffStrategy
from forze.application.execution.resilience.backoff import compute_delay, retry_after
from forze.base.exceptions import CoreException, exc

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


@pytest.mark.parametrize("jitter", ["none", "full", "equal", "decorrelated"])
@pytest.mark.parametrize("attempt", [1, 2, 1024, 1025, 10_000])
def test_cap_holds_for_any_attempt(jitter: str, attempt: int) -> None:
    # The exponential term overflows a float long before a retry loop with a
    # large max_attempts (or an outbox row's attempt count) stops growing.
    delay = compute_delay(_backoff(jitter), attempt, 0.0, random.Random(4))
    assert 0.0 <= delay <= 10.0


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (exc.throttled("slow", details={"retry_after": 5}), 5.0),
        (exc.throttled("slow", details={"retry_after": 0.5}), 0.5),
        (exc.throttled("slow", details={"retry_after": 0}), 0.0),
        (exc.throttled("slow"), None),
        (exc.throttled("slow", details={"retry_after": True}), None),
        (exc.throttled("slow", details={"retry_after": -1}), None),
        (exc.throttled("slow", details={"retry_after": "5"}), None),
        (exc.throttled("slow", details={"retry_after": float("nan")}), None),
        (exc.throttled("slow", details={"retry_after": float("inf")}), math.inf),
        (exc.throttled("slow", details={"retry_after": 10**400}), math.inf),
        (exc.unavailable("off", details={"retry_after": 30}), 30.0),
        (exc.infrastructure("down", details={"retry_after": 5}), None),
    ],
)
def test_retry_after_reads_only_a_hinted_kinds_numeric_hint(
    error: CoreException, expected: float | None
) -> None:
    assert retry_after(error) == expected
