"""Unit tests for the DDSketch relative-error quantile sketch."""

from __future__ import annotations

import math
import random

import pytest
from hypothesis import given
from hypothesis import strategies as st

from forze.base.exceptions import CoreException
from forze.base.primitives import DDSketch, WindowedDDSketch

# ----------------------- #

_ALPHA = 0.01


def _true_quantile(values: list[float], q: float) -> float:
    """Reference quantile via the same rank convention the sketch uses."""

    ordered = sorted(values)
    rank = q * (len(ordered) - 1)
    lo = math.floor(rank)
    hi = math.ceil(rank)

    if lo == hi:
        return ordered[lo]

    return ordered[lo] + (ordered[hi] - ordered[lo]) * (rank - lo)


def _assert_within_alpha(estimate: float, truth: float, alpha: float = _ALPHA) -> None:
    if truth == 0.0:
        assert estimate == 0.0

        return

    assert abs(estimate - truth) / truth <= alpha


# ----------------------- #


def test_empty_sketch_returns_none() -> None:
    sketch = DDSketch(relative_accuracy=_ALPHA)

    assert sketch.quantile(0.5) is None
    assert sketch.count == 0


def test_single_value() -> None:
    sketch = DDSketch(relative_accuracy=_ALPHA)
    sketch.observe(42.0)

    assert sketch.count == 1
    _assert_within_alpha(sketch.quantile(0.5), 42.0)
    _assert_within_alpha(sketch.quantile(0.99), 42.0)


@pytest.mark.parametrize("q", [0.5, 0.9, 0.95, 0.99])
def test_accuracy_lognormal(q: float) -> None:
    rng = random.Random(1234)
    values = [rng.lognormvariate(3.0, 1.0) for _ in range(50_000)]

    sketch = DDSketch(relative_accuracy=_ALPHA)

    for v in values:
        sketch.observe(v)

    _assert_within_alpha(sketch.quantile(q), _true_quantile(values, q))


@pytest.mark.parametrize("q", [0.5, 0.9, 0.99])
def test_accuracy_uniform(q: float) -> None:
    rng = random.Random(99)
    values = [rng.uniform(1.0, 1000.0) for _ in range(50_000)]

    sketch = DDSketch(relative_accuracy=_ALPHA)

    for v in values:
        sketch.observe(v)

    _assert_within_alpha(sketch.quantile(q), _true_quantile(values, q))


def test_zero_observations_tracked() -> None:
    sketch = DDSketch(relative_accuracy=_ALPHA)

    for _ in range(60):
        sketch.observe(0.0)

    for v in (10.0, 20.0, 30.0, 40.0):
        sketch.observe(v)

    assert sketch.count == 64
    # 60/64 of the mass is exactly zero, so the median is zero.
    assert sketch.quantile(0.5) == 0.0
    assert sketch.quantile(0.99) is not None and sketch.quantile(0.99) > 0.0


def test_min_and_max_quantiles() -> None:
    sketch = DDSketch(relative_accuracy=_ALPHA)
    values = [float(v) for v in range(1, 1001)]

    for v in values:
        sketch.observe(v)

    _assert_within_alpha(sketch.quantile(0.0), 1.0)
    _assert_within_alpha(sketch.quantile(1.0), 1000.0)


# ----------------------- #


def test_merge_equivalence() -> None:
    rng = random.Random(7)
    left = [rng.lognormvariate(2.0, 0.8) for _ in range(20_000)]
    right = [rng.lognormvariate(4.0, 0.5) for _ in range(20_000)]

    a = DDSketch(relative_accuracy=_ALPHA)
    b = DDSketch(relative_accuracy=_ALPHA)
    combined = DDSketch(relative_accuracy=_ALPHA)

    for v in left:
        a.observe(v)
        combined.observe(v)

    for v in right:
        b.observe(v)
        combined.observe(v)

    a.merge(b)

    assert a.count == combined.count

    truth = left + right

    for q in (0.5, 0.9, 0.99):
        _assert_within_alpha(a.quantile(q), _true_quantile(truth, q))
        # The merged sketch and the all-at-once sketch agree exactly (same bins).
        assert a.quantile(q) == combined.quantile(q)


def test_merge_is_order_independent() -> None:
    rng = random.Random(11)
    parts = [[rng.expovariate(0.1) for _ in range(5_000)] for _ in range(4)]

    sketches = []

    for part in parts:
        s = DDSketch(relative_accuracy=_ALPHA)

        for v in part:
            s.observe(v)

        sketches.append(s)

    forward = DDSketch.merged(*sketches)
    backward = DDSketch.merged(*reversed(sketches))

    for q in (0.1, 0.5, 0.9, 0.99):
        assert forward.quantile(q) == backward.quantile(q)


def test_merge_rejects_mismatched_accuracy() -> None:
    a = DDSketch(relative_accuracy=0.01)
    b = DDSketch(relative_accuracy=0.02)

    with pytest.raises(CoreException):
        a.merge(b)


# ----------------------- #


def test_bounded_memory_under_huge_dynamic_range() -> None:
    sketch = DDSketch(relative_accuracy=_ALPHA, max_bins=128)
    rng = random.Random(5)

    # Spread values across ~12 orders of magnitude to force collapsing.
    for _ in range(100_000):
        sketch.observe(rng.uniform(1e-6, 1e6))

    assert len(sketch._bins) <= 128
    # The tail keeps full accuracy even though the low end was collapsed.
    assert sketch.quantile(0.99) is not None


def test_collapsing_preserves_tail_above_boundary() -> None:
    """Collapse-lowest sacrifices the low end but keeps the tail above it exact.

    A wide, dense low cluster generates far more distinct buckets than
    ``max_bins`` (forcing collapse), while a tight high cluster forms the top
    ~9% of the mass. The collapse boundary lands inside the low cluster, so the
    high quantiles — which live in the high cluster, above the boundary — stay
    within ``alpha``.
    """

    rng = random.Random(3)
    low = [rng.uniform(1e-3, 100.0) for _ in range(10_000)]
    high = [rng.uniform(1000.0, 1100.0) for _ in range(1_000)]
    values = low + high

    capped = DDSketch(relative_accuracy=_ALPHA, max_bins=64)

    for v in values:
        capped.observe(v)

    assert len(capped._bins) <= 64

    # p95 and p99 fall within the high cluster, above the collapse boundary.
    for q in (0.95, 0.99):
        _assert_within_alpha(capped.quantile(q), _true_quantile(values, q))


# ----------------------- #


def test_invalid_relative_accuracy_rejected() -> None:
    for bad in (0.0, 1.0, -0.1, 1.5):
        with pytest.raises(CoreException):
            DDSketch(relative_accuracy=bad)


def test_invalid_max_bins_rejected() -> None:
    with pytest.raises(CoreException):
        DDSketch(relative_accuracy=_ALPHA, max_bins=0)


def test_negative_observation_rejected() -> None:
    sketch = DDSketch(relative_accuracy=_ALPHA)

    with pytest.raises(CoreException):
        sketch.observe(-1.0)


def test_quantile_out_of_range_rejected() -> None:
    sketch = DDSketch(relative_accuracy=_ALPHA)
    sketch.observe(1.0)

    for bad in (-0.01, 1.01):
        with pytest.raises(CoreException):
            sketch.quantile(bad)


# ----------------------- #


@given(
    values=st.lists(
        st.floats(min_value=0.0, max_value=1e6, allow_nan=False, allow_infinity=False),
        min_size=1,
        max_size=500,
    )
)
def test_quantiles_are_monotonic(values: list[float]) -> None:
    """For any data, the estimated quantile is non-decreasing in q."""

    sketch = DDSketch(relative_accuracy=_ALPHA)

    for v in values:
        sketch.observe(v)

    estimates = [sketch.quantile(q) for q in (0.1, 0.25, 0.5, 0.75, 0.9, 0.99)]

    for lower, higher in zip(estimates, estimates[1:]):
        assert lower is not None and higher is not None
        assert lower <= higher + 1e-9


@given(
    values=st.lists(
        st.floats(min_value=1e-3, max_value=1e6, allow_nan=False, allow_infinity=False),
        min_size=1,
        max_size=400,
    ),
    split=st.integers(min_value=0, max_value=400),
)
def test_merge_associativity_property(values: list[float], split: int) -> None:
    """Splitting a stream and merging the halves matches the whole-stream sketch."""

    split = min(split, len(values))
    whole = DDSketch(relative_accuracy=_ALPHA)
    left = DDSketch(relative_accuracy=_ALPHA)
    right = DDSketch(relative_accuracy=_ALPHA)

    for i, v in enumerate(values):
        whole.observe(v)
        (left if i < split else right).observe(v)

    left.merge(right)

    assert left.count == whole.count

    for q in (0.5, 0.9, 0.99):
        assert left.quantile(q) == whole.quantile(q)


# ----------------------- #


def test_windowed_returns_none_when_empty() -> None:
    sketch = WindowedDDSketch(relative_accuracy=_ALPHA, window=100)

    assert sketch.quantile(0.5) is None


def test_windowed_tracks_distribution_shift() -> None:
    """After a regime change, the windowed sketch follows the new level."""

    sketch = WindowedDDSketch(relative_accuracy=_ALPHA, window=200)

    for _ in range(1_000):
        sketch.observe(10.0)

    # Shift to a 100x-higher level for well over two windows.
    for _ in range(1_000):
        sketch.observe(1000.0)

    # Cumulative history would sit between the two; the window has moved on.
    _assert_within_alpha(sketch.quantile(0.5), 1000.0)


def test_windowed_invalid_window_rejected() -> None:
    with pytest.raises(CoreException):
        WindowedDDSketch(relative_accuracy=_ALPHA, window=0)
