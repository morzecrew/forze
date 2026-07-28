"""The detection-statistics kernel — exact bounds, survival curves, and the locked verdict.

Statistical code with wrong constants is worse than none, so the kernel is pinned against
closed forms and published worked examples: the Gehan 6-MP leukemia data for Kaplan–Meier and
the log-rank test, classic Clopper–Pearson intervals, and the exactness property itself (the
binomial tail at the bound equals α/2).
"""

import math
from types import MappingProxyType

import pytest

from forze_dst.artifacts.sweep import SweepResult
from forze_dst.oracle.confidence import ConfidenceReport, assess_confidence
from forze_dst.oracle.coverage import CoverageStats
from forze_dst.oracle.invariants import Violation
from forze_dst.oracle.recorder import History
from forze_dst.oracle.replay import ViolationReport
from forze_dst.stats import (
    SurvivalCurve,
    binomial_ci,
    detection_upper_bound,
    format_clean_verdict,
    geometric_p_hat,
    log_rank,
)

# The Gehan (1965) 6-MP leukemia trial — the canonical Kaplan–Meier worked example. Remission
# lengths in weeks; the treated arm is right-censored, the placebo arm has no censoring.
_SIX_MP_EVENTS = (6, 6, 6, 7, 10, 13, 16, 22, 23)
_SIX_MP_CENSORED = (6, 9, 10, 11, 17, 19, 20, 25, 32, 32, 34, 35)
_PLACEBO_EVENTS = (1, 1, 2, 2, 3, 4, 4, 5, 5, 8, 8, 8, 8, 11, 11, 12, 12, 15, 17, 22, 23)

# ----------------------- #


class TestDetectionUpperBound:
    def test_exact_closed_form(self) -> None:
        # 1 - (1 - γ)^(1/S): the largest p under which S clean seeds are still plausible at γ.
        assert detection_upper_bound(1000) == pytest.approx(1.0 - 0.05 ** (1 / 1000))
        assert detection_upper_bound(50, confidence=0.99) == pytest.approx(1.0 - 0.01 ** (1 / 50))

    def test_single_clean_run_excludes_only_the_confidence_level(self) -> None:
        assert detection_upper_bound(1) == pytest.approx(0.95)

    def test_rule_of_three_is_the_mnemonic_not_the_value(self) -> None:
        # The exact bound sits just under 3/S (ln 20 ≈ 2.9957), which is why 3/S works as
        # the mental shortcut — and why we print the exact form.
        for runs in (10, 100, 1000, 10_000):
            exact = detection_upper_bound(runs)
            assert exact < 3.0 / runs
            if runs >= 100:
                assert exact > 2.9 / runs

    def test_monotone_more_seeds_exclude_more(self) -> None:
        assert detection_upper_bound(10) > detection_upper_bound(100) > detection_upper_bound(1000)

    def test_rejects_nonpositive_runs(self) -> None:
        with pytest.raises(ValueError, match="runs must be >= 1"):
            detection_upper_bound(0)

    def test_rejects_degenerate_confidence(self) -> None:
        with pytest.raises(ValueError, match="confidence must be in"):
            detection_upper_bound(10, confidence=1.0)
        with pytest.raises(ValueError, match="confidence must be in"):
            detection_upper_bound(10, confidence=0.0)


# ....................... #


class TestFormatCleanVerdict:
    def test_locked_sentence_carries_bound_confidence_and_scope(self) -> None:
        out = format_clean_verdict(1000)

        assert "0 violations in 1000 seeds" in out
        assert "< 0.30%" in out  # 1 - 0.05^(1/1000) ≈ 0.2991% rendered at two decimals
        assert "(95%, exact)" in out
        # The scope clause is part of the sentence — the number never travels without it.
        assert "for this scenario × strategy × oracle set (independent seeds)" in out

    def test_singular_seed(self) -> None:
        assert "0 violations in 1 seed →" in format_clean_verdict(1)

    def test_non_default_confidence_is_stated(self) -> None:
        assert "(99%, exact)" in format_clean_verdict(100, confidence=0.99)

    def test_high_precision_confidence_never_rounds_to_100(self) -> None:
        out = format_clean_verdict(1000, confidence=0.999)

        assert "(99.9%, exact)" in out
        assert "100%" not in out

    def test_tiny_bound_falls_back_to_scientific_never_zero_percent(self) -> None:
        out = format_clean_verdict(10_000_000)

        assert "0.00%" not in out
        assert "e-07" in out


# ....................... #


class TestBinomialCi:
    def test_classic_one_of_ten(self) -> None:
        # The textbook Clopper–Pearson interval for 1 success in 10 trials at 95%.
        ci = binomial_ci(1, 10)

        assert ci.lower == pytest.approx(0.00253, abs=5e-5)
        assert ci.upper == pytest.approx(0.44502, abs=5e-5)

    def test_zero_event_edges_are_closed_form(self) -> None:
        ci = binomial_ci(0, 50)

        assert ci.lower == 0.0
        assert ci.upper == pytest.approx(1.0 - 0.025 ** (1 / 50))

    def test_all_events_mirror_zero_events(self) -> None:
        zero, full = binomial_ci(0, 50), binomial_ci(50, 50)

        assert full.upper == 1.0
        assert full.lower == pytest.approx(1.0 - zero.upper)

    def test_detection_upper_bound_is_the_one_sided_special_case(self) -> None:
        # A two-sided 90% interval's upper limit IS the one-sided 95% bound.
        assert binomial_ci(0, 200, confidence=0.90).upper == pytest.approx(
            detection_upper_bound(200, confidence=0.95)
        )

    def test_exactness_the_binomial_tail_at_the_bound_equals_alpha_halves(self) -> None:
        # The defining property of Clopper–Pearson: at p = lower, P(X >= k) = α/2.
        k, n = 3, 20
        ci = binomial_ci(k, n)

        tail = sum(
            math.comb(n, i) * ci.lower**i * (1.0 - ci.lower) ** (n - i) for i in range(k, n + 1)
        )
        assert tail == pytest.approx(0.025, abs=1e-6)

    def test_validation(self) -> None:
        with pytest.raises(ValueError, match="trials"):
            binomial_ci(0, 0)
        with pytest.raises(ValueError, match="successes"):
            binomial_ci(5, 4)


# ....................... #


class TestSurvivalCurve:
    def test_six_mp_reproduces_the_published_steps(self) -> None:
        curve = SurvivalCurve.fit(_SIX_MP_EVENTS, _SIX_MP_CENSORED)

        published = {6: 0.857, 7: 0.807, 10: 0.753, 13: 0.690, 16: 0.627, 22: 0.538, 23: 0.448}
        assert {step.time for step in curve.steps} == set(published)
        for step in curve.steps:
            assert step.survival == pytest.approx(published[step.time], abs=5e-4), step.time
        assert curve.n_runs == 21
        assert curve.n_censored == 12

    def test_six_mp_median_is_the_published_twenty_three(self) -> None:
        assert SurvivalCurve.fit(_SIX_MP_EVENTS, _SIX_MP_CENSORED).median == 23

    def test_heavy_censoring_yields_an_honest_none(self) -> None:
        curve = SurvivalCurve.fit([1], [10] * 9)

        assert curve.survival_at(1) == pytest.approx(0.9)
        assert curve.median is None  # the curve never reaches 0.5 — no median claim

    def test_greenwood_band_matches_hand_computation(self) -> None:
        # One detection among four runs: S = 0.75, se = 0.75·√(1/12), z = 1.95996….
        curve = SurvivalCurve.fit([1], [2, 2, 2])

        (step,) = curve.steps
        se = 0.75 * math.sqrt(1.0 / 12.0)
        assert step.survival == pytest.approx(0.75)
        assert step.lower == pytest.approx(0.75 - 1.959964 * se, abs=1e-5)
        assert step.upper == 1.0  # capped

    def test_survival_at_steps_between_event_times(self) -> None:
        curve = SurvivalCurve.fit([2, 4], [5, 5])

        assert curve.survival_at(1) == 1.0
        assert curve.survival_at(3) == pytest.approx(0.75)
        assert curve.survival_at(10) == pytest.approx(0.5)

    def test_format_shows_quantiles_never_a_mean(self) -> None:
        out = SurvivalCurve.fit(_SIX_MP_EVENTS, _SIX_MP_CENSORED).format()

        assert "median:   23" in out
        assert "censored" in out
        assert "mean" not in out

    def test_validation(self) -> None:
        with pytest.raises(ValueError, match="at least one observation"):
            SurvivalCurve.fit([], [])
        with pytest.raises(ValueError, match=">= 1"):
            SurvivalCurve.fit([0])


# ....................... #


class TestLogRank:
    def test_six_mp_versus_placebo_reproduces_the_published_statistic(self) -> None:
        result = log_rank(_SIX_MP_EVENTS, _SIX_MP_CENSORED, _PLACEBO_EVENTS, ())

        assert result.statistic == pytest.approx(16.79, rel=0.01)
        assert result.p_value < 1e-4

    def test_identical_groups_show_no_difference(self) -> None:
        result = log_rank([3, 5, 8], [10], [3, 5, 8], [10])

        assert result.statistic == pytest.approx(0.0, abs=1e-12)
        assert result.p_value == pytest.approx(1.0)

    def test_validation(self) -> None:
        with pytest.raises(ValueError, match="at least one detection"):
            log_rank([], [5], [], [5])


# ....................... #


class TestGeometricPHat:
    def test_mle_is_detections_over_total_seeds(self) -> None:
        estimate = geometric_p_hat([1, 2, 3], [10])

        assert estimate.p_hat == pytest.approx(3 / 16)
        assert estimate.detections == 3
        assert estimate.seeds == 16
        assert estimate.ci.lower < estimate.p_hat < estimate.ci.upper

    def test_zero_detections_bound_matches_the_clean_run_verdict(self) -> None:
        estimate = geometric_p_hat([], [100] * 10, confidence=0.90)

        assert estimate.p_hat == 0.0
        assert estimate.ci.upper == pytest.approx(detection_upper_bound(1000, confidence=0.95))

    def test_validation(self) -> None:
        with pytest.raises(ValueError, match="at least one observation"):
            geometric_p_hat([], [])


# ....................... #


class TestSweepResultVerdictLine:
    def _result(self, *, runs: int, violations: tuple[int, ...]) -> SweepResult:
        return SweepResult(
            runs=runs,
            violations=violations,
            behaviors=frozenset(),
            reached_runs=MappingProxyType({}),
            simulated_seconds=0.0,
            wall_seconds=1.0,
        )

    def test_clean_sweep_prints_the_bound(self) -> None:
        out = self._result(runs=10, violations=()).format()

        assert "✓ 0 violations in 10 seeds" in out
        assert "per-seed detection probability" in out

    def test_violating_sweep_never_prints_the_bound(self) -> None:
        out = self._result(runs=10, violations=(3,)).format()

        assert "✗ violations:" in out
        assert "per-seed detection probability" not in out

    def test_empty_sweep_prints_neither(self) -> None:
        out = self._result(runs=0, violations=()).format()

        assert "per-seed detection probability" not in out


# ....................... #


class TestCoverageStatsVerdictLine:
    def _stats(self, **overrides: object) -> CoverageStats:
        base: dict[str, object] = {
            "behaviors": frozenset(),
            "seeds_run": 5,
            "new_by_seed": (),
            "plateaued": False,
        }
        base.update(overrides)
        return CoverageStats(**base)  # type: ignore[arg-type]

    def test_clean_coverage_prints_the_bound(self) -> None:
        out = self._stats().format()

        assert "✓ 0 violations in 5 seeds" in out

    def test_violation_suppresses_the_bound(self) -> None:
        report = ViolationReport(
            seed=3,
            schedule_seed=None,
            violations=(Violation(invariant="conservation", message="lost deposit"),),
            workload=(),
            history=History(seed=3, events=()),
        )

        out = self._stats(violation=report).format()

        assert "✗ violation at seed 3" in out
        assert "per-seed detection probability" not in out

    def test_zero_seeds_run_prints_no_bound(self) -> None:
        assert "per-seed detection probability" not in self._stats(seeds_run=0).format()


# ....................... #


class TestConfidenceReportVerdictLine:
    def test_clean_report_prints_the_bound(self) -> None:
        report = ConfidenceReport(seeds_run=7, ran_ops=("a",), raced_ops=("a",))

        out = report.format()

        assert "✓ 0 violations in 7 seeds" in out

    def test_bound_sits_adjacent_to_vacuity_warnings(self) -> None:
        # A vacuous-but-clean sweep still gets the bound — with the gap warning right beside
        # it, so the number is never read as stronger than the coverage supports.
        report = ConfidenceReport(seeds_run=7, ran_ops=("a", "b"), raced_ops=("a",))

        out = report.format()

        assert "⚠ confidence gaps:" in out
        assert "✓ 0 violations in 7 seeds" in out

    def test_violations_seen_suppresses_the_bound(self) -> None:
        report = ConfidenceReport(
            seeds_run=7, ran_ops=("a",), raced_ops=("a",), violations_seen=1
        )

        assert "per-seed detection probability" not in report.format()

    def test_assess_confidence_threads_violations(self) -> None:
        clean = assess_confidence([History(seed=0, events=())])
        dirty = assess_confidence([History(seed=0, events=())], violations=1)

        assert clean.violations_seen == 0
        assert "per-seed detection probability" in clean.format()
        assert dirty.violations_seen == 1
        assert "per-seed detection probability" not in dirty.format()
