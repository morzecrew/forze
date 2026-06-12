"""P² streaming quantile estimation: accuracy, warmup, windowed rotation."""

from __future__ import annotations

import random

from forze.application.execution.resilience.quantile import (
    P2Quantile,
    WindowedP2Quantile,
)

# ----------------------- #


def _true_quantile(values: list[float], p: float) -> float:
    ordered = sorted(values)
    idx = min(len(ordered) - 1, max(0, round(p * (len(ordered) - 1))))

    return ordered[idx]


# ----------------------- #


class TestP2Quantile:
    def test_undefined_before_five_observations(self) -> None:
        est = P2Quantile(p=0.95)

        for x in (3.0, 1.0, 2.0, 4.0):
            est.observe(x)
            assert est.value() is None

        est.observe(5.0)
        assert est.value() is not None

    def test_five_samples_seed_exact_median(self) -> None:
        est = P2Quantile(p=0.5)

        for x in (9.0, 1.0, 5.0, 3.0, 7.0):
            est.observe(x)

        assert est.value() == 5.0  # middle marker of the sorted seed

    def test_tracks_p95_of_uniform_stream(self) -> None:
        rng = random.Random(42)
        values = [rng.uniform(0.0, 100.0) for _ in range(2000)]
        est = P2Quantile(p=0.95)

        for x in values:
            est.observe(x)

        truth = _true_quantile(values, 0.95)
        estimate = est.value()
        assert estimate is not None
        assert abs(estimate - truth) <= 3.0  # within a few percent of the range

    def test_tracks_p95_of_heavy_tailed_stream(self) -> None:
        rng = random.Random(7)
        values = [rng.lognormvariate(0.0, 1.0) for _ in range(5000)]
        est = P2Quantile(p=0.95)

        for x in values:
            est.observe(x)

        truth = _true_quantile(values, 0.95)
        estimate = est.value()
        assert estimate is not None
        assert abs(estimate - truth) / truth <= 0.15

    def test_tracks_median_of_sequential_stream(self) -> None:
        # No randomness at all: 1..999 in order.
        est = P2Quantile(p=0.5)

        for x in range(1, 1000):
            est.observe(float(x))

        estimate = est.value()
        assert estimate is not None
        assert abs(estimate - 500.0) <= 20.0

    def test_count_increments(self) -> None:
        est = P2Quantile(p=0.9)

        for x in range(10):
            est.observe(float(x))

        assert est.count == 10


class TestWindowedP2Quantile:
    def test_serves_none_before_warmup(self) -> None:
        est = WindowedP2Quantile(p=0.95, window=50)

        for x in (1.0, 2.0, 3.0, 4.0):
            est.observe(x)

        assert est.value() is None

    def test_distribution_shift_reflected_within_two_windows(self) -> None:
        est = WindowedP2Quantile(p=0.95, window=100)

        for _ in range(200):
            est.observe(0.01)  # a fast downstream

        before = est.value()
        assert before is not None
        assert before <= 0.02

        for _ in range(200):  # two full windows of the new, slower regime
            est.observe(1.0)

        after = est.value()
        assert after is not None
        assert after >= 0.9  # old history fully rotated out

    def test_matches_plain_p2_within_first_window(self) -> None:
        rng = random.Random(3)
        values = [rng.uniform(0.0, 10.0) for _ in range(80)]

        windowed = WindowedP2Quantile(p=0.5, window=100)
        plain = P2Quantile(p=0.5)

        for x in values:
            windowed.observe(x)
            plain.observe(x)

        assert windowed.value() == plain.value()
