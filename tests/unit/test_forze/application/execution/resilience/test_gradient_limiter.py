"""Gradient2 delay-based concurrency limiter: convergence, contraction, guards."""

from __future__ import annotations

import pytest
from hypothesis import given
from hypothesis import strategies as st

from forze.application.execution.resilience.limiter import Gradient2Limiter
from forze.base.exceptions import CoreException

# ----------------------- #


def _limiter(**kw: object) -> Gradient2Limiter:
    params: dict[str, object] = {
        "initial_limit": 10,
        "max_limit": 100,
        "min_limit": 1,
        "long_window": 100,
    }
    params.update(kw)

    return Gradient2Limiter(**params)  # type: ignore[arg-type]


# ----------------------- #


class TestConvergence:
    def test_healthy_latency_ramps_limit_toward_max(self) -> None:
        limiter = _limiter()

        # Steady baseline latency, fully loaded: the limit should climb.
        for _ in range(2000):
            limiter.observe(rtt=0.01, inflight=limiter.limit)

        assert limiter.limit == 100  # clamped at the ceiling

    def test_ramp_is_gradual_not_a_jump(self) -> None:
        limiter = _limiter(initial_limit=10, smoothing=0.2, queue_size=4.0)

        before = limiter.limit
        limiter.observe(rtt=0.01, inflight=10)
        after = limiter.limit

        # One healthy step adds at most ~smoothing * queue_size, not a jump.
        assert before <= after <= before + 2


class TestContraction:
    def test_latency_inflation_shrinks_limit(self) -> None:
        limiter = _limiter(initial_limit=80)

        # Warm the baseline at a low latency under load.
        for _ in range(200):
            limiter.observe(rtt=0.01, inflight=80)

        warmed = limiter.limit

        # Latency inflates 10x: the limit must contract.
        for _ in range(20):
            limiter.observe(rtt=0.1, inflight=limiter.limit)

        assert limiter.limit < warmed

    def test_contraction_respects_min_limit(self) -> None:
        limiter = _limiter(initial_limit=50, min_limit=3)

        for _ in range(50):
            limiter.observe(rtt=0.01, inflight=50)

        # Sustained severe latency inflation.
        for _ in range(200):
            limiter.observe(rtt=5.0, inflight=limiter.limit)

        assert limiter.limit >= 3

    def test_single_step_contraction_is_bounded(self) -> None:
        limiter = _limiter(initial_limit=80)

        for _ in range(200):
            limiter.observe(rtt=0.01, inflight=80)

        before = limiter.limit
        # Even an extreme spike cannot more than roughly halve in one step
        # (the gradient floors at 0.5).
        limiter.observe(rtt=100.0, inflight=before)

        assert limiter.limit >= before * 0.5


class TestNoLoadGuard:
    def test_low_inflight_never_grows_the_limit(self) -> None:
        limiter = _limiter(initial_limit=10)

        # Healthy latency but barely any concurrency: nothing to probe with.
        for _ in range(1000):
            limiter.observe(rtt=0.01, inflight=1)

        assert limiter.limit == 10

    def test_baseline_still_tracks_under_no_load(self) -> None:
        limiter = _limiter(initial_limit=10, long_window=10)

        limiter.observe(rtt=0.05, inflight=0)

        # The baseline learns from the sample even when the limit is held.
        assert limiter.baseline_rtt == pytest.approx(0.05)


class TestValidation:
    def test_rejects_invalid_params(self) -> None:
        for kw in (
            {"min_limit": 0},
            {"max_limit": 0},  # < min_limit
            {"initial_limit": 200},  # > max_limit
            {"rtt_tolerance": 0.5},
            {"smoothing": 0.0},
            {"smoothing": 1.5},
            {"long_window": 0},
            {"queue_size": -1.0},
        ):
            with pytest.raises(CoreException):
                _limiter(**kw)

    def test_rejects_non_positive_rtt(self) -> None:
        limiter = _limiter()

        for bad in (0.0, -0.01):
            with pytest.raises(CoreException):
                limiter.observe(rtt=bad, inflight=5)


class TestProperties:
    @given(
        samples=st.lists(
            st.tuples(
                st.floats(
                    min_value=1e-4,
                    max_value=10.0,
                    allow_nan=False,
                    allow_infinity=False,
                ),
                st.integers(min_value=0, max_value=200),
            ),
            min_size=1,
            max_size=500,
        )
    )
    def test_limit_always_within_bounds(self, samples: list[tuple[float, int]]) -> None:
        limiter = _limiter(initial_limit=10, min_limit=2, max_limit=100)

        for rtt, inflight in samples:
            limiter.observe(rtt=rtt, inflight=inflight)
            assert 2 <= limiter.limit <= 100

    def test_is_deterministic(self) -> None:
        a = _limiter(initial_limit=20)
        b = _limiter(initial_limit=20)

        for i in range(300):
            rtt = 0.01 + (i % 7) * 0.002
            inflight = 10 + (i % 13)
            assert a.observe(rtt=rtt, inflight=inflight) == b.observe(
                rtt=rtt, inflight=inflight
            )
