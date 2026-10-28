"""Tests for backoff delay computation and jitter modes."""

from __future__ import annotations

import math
import random
from datetime import UTC, datetime, timedelta, timezone

import pytest

from forze.application.contracts.resilience import BackoffStrategy
from forze.application.execution.resilience.backoff import compute_delay, retry_after, retry_hint
from forze.base.exceptions import CoreException, exc
from forze.base.primitives import FrozenTimeSource, bind_time_source

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


# ....................... #

NOW = datetime(2026, 10, 21, 7, 28, tzinfo=UTC)


@pytest.mark.parametrize(
    ("details", "expected"),
    [
        ({"retry_at": NOW + timedelta(seconds=90)}, 90.0),
        # Any aware zone reads as the same instant.
        ({"retry_at": datetime(2026, 10, 21, 10, 29, tzinfo=timezone(timedelta(hours=3)))}, 60.0),
        ({"retry_at": NOW - timedelta(seconds=5)}, 0.0),
        ({"retry_after": 5, "retry_at": NOW + timedelta(seconds=90)}, 5.0),
        # An unusable or infinite retry_after falls through to retry_at, as the header does.
        ({"retry_after": -1, "retry_at": NOW + timedelta(seconds=90)}, 90.0),
        ({"retry_after": float("inf"), "retry_at": NOW + timedelta(seconds=90)}, 90.0),
        ({"retry_at": datetime(2026, 10, 21, 7, 30)}, None),
        ({"retry_at": "2026-10-21T07:30:00+00:00"}, None),
        ({"retry_at": datetime(1, 1, 1, tzinfo=timezone(timedelta(hours=1)))}, None),
    ],
    ids=[
        "future",
        "other-zone",
        "past",
        "retry-after-wins",
        "unusable-seconds",
        "infinite-seconds",
        "naive",
        "string",
        "out-of-range",
    ],
)
def test_retry_after_reads_a_retry_at_instant(
    details: dict[str, object], expected: float | None
) -> None:
    with bind_time_source(FrozenTimeSource(NOW)):
        assert retry_after(exc.unavailable("off", details=details)) == expected
        assert retry_after(exc.throttled("slow", details=details)) == expected


def test_retry_hint_keeps_the_instant_in_utc() -> None:
    at = datetime(2026, 10, 21, 10, 28, tzinfo=timezone(timedelta(hours=3)))

    hint = retry_hint(exc.throttled("slow", details={"retry_at": at}))

    assert hint == at
    assert isinstance(hint, datetime) and hint.tzinfo is UTC
    assert retry_hint(exc.infrastructure("down", details={"retry_at": at})) is None
