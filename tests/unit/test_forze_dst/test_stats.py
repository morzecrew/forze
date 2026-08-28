"""The detection-statistics kernel — exact bounds, survival curves, and the locked verdict.

Statistical code with wrong constants is worse than none, so the kernel is pinned against
closed forms and published worked examples: the Gehan 6-MP leukemia data for Kaplan–Meier and
the log-rank test, classic Clopper–Pearson intervals, and the exactness property itself (the
binomial tail at the bound equals α/2).
"""

import math
import random
from collections import Counter
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
    _render_probability,
    binomial_ci,
    coverage_deficit,
    detection_upper_bound,
    familywise_level,
    fisher_exact,
    flip_margin,
    format_clean_verdict,
    format_coverage_deficit,
    format_family_verdict,
    format_withheld_verdict,
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


class TestFormatWithheldVerdict:
    """The stopping-time refusal: an exact bound is a *fixed-design* guarantee, so a sweep whose
    ``n`` was read off its own runs states the stop reason and prints no number."""

    def test_states_the_stop_reason_and_carries_no_bound(self) -> None:
        out = format_withheld_verdict(13, stop_reason="plateau stop")

        assert "0 violations in 13 seeds" in out
        assert "no per-seed bound" in out
        assert "n was chosen from the data" in out
        assert "plateau stop" in out
        # Nothing that could be quoted as a rate: no percentage, no confidence, no "exact".
        assert "%" not in out
        assert "detection probability" not in out
        assert "exact)" not in out

    def test_points_at_the_fixed_design_alternative(self) -> None:
        # Withholding without a remedy would just be a dead end; audit() is the fixed-n path.
        assert "audit()" in format_withheld_verdict(13, stop_reason="plateau stop")

    def test_singular_seed(self) -> None:
        assert "0 violations in 1 seed →" in format_withheld_verdict(1, stop_reason="plateau stop")

    def test_rejects_nonpositive_runs(self) -> None:
        with pytest.raises(ValueError, match="runs must be >= 1"):
            format_withheld_verdict(0, stop_reason="plateau stop")


# ....................... #


class TestCoverageDeficit:
    """Chao1 + Good–Turing over a frequency table. Pinned to the closed form, to recovery of a
    known richness, and to *silence* on a source that genuinely has one shape."""

    def test_closed_form_chao1(self) -> None:
        # S_obs + f1²/(2·f2): 50 + 100/10 = 60.
        counts = {f"s{i}": 1 for i in range(10)}
        counts |= {f"s{i}": 2 for i in range(10, 15)}
        counts |= {f"s{i}": 7 for i in range(15, 50)}

        deficit = coverage_deficit(counts)

        assert deficit.observed == 50
        assert (deficit.singletons, deficit.doubletons) == (10, 5)
        assert deficit.richness == pytest.approx(60.0)
        assert deficit.unseen == pytest.approx(10.0)
        # Good–Turing: 10 singletons over 10 + 10 + 245 = 265 observations.
        assert deficit.unseen_mass == pytest.approx(10 / 265)

    def test_interval_brackets_the_estimate(self) -> None:
        counts = {f"s{i}": 1 for i in range(10)} | {f"s{i}": 2 for i in range(10, 15)}

        deficit = coverage_deficit(counts)

        # The log transform keeps the interval above what was actually counted.
        assert deficit.observed <= deficit.lower <= deficit.richness <= deficit.upper

    def test_recovers_a_known_richness_from_an_undersampled_source(self) -> None:
        # A synthetic run-source with a fixed shape population: sampled below saturation the truth
        # sits inside the interval, and the estimate is a *lower* bound on it (the safe direction).
        rng = random.Random(7)
        truth = 80

        undersampled = coverage_deficit(Counter(rng.randrange(truth) for _ in range(150)))

        assert undersampled.observed < truth  # genuinely below saturation
        assert undersampled.richness > undersampled.observed  # it names the deficit
        assert undersampled.lower <= truth <= undersampled.upper

    def test_sampled_to_exhaustion_the_estimate_equals_the_truth(self) -> None:
        rng = random.Random(7)
        truth = 80

        exhausted = coverage_deficit(Counter(rng.randrange(truth) for _ in range(4000)))

        assert exhausted.observed == truth
        assert exhausted.richness == pytest.approx(float(truth))
        assert exhausted.unseen == 0.0

    def test_does_not_cry_wolf_on_a_degenerate_source(self) -> None:
        # One shape, three hundred runs: richness 1, no deficit, no interval to wave around.
        deficit = coverage_deficit({"one-shape": 300})

        assert deficit.observed == 1
        assert deficit.richness == 1.0
        assert deficit.unseen == 0.0
        assert deficit.unseen_mass == 0.0
        assert (deficit.lower, deficit.upper) == (1.0, 1.0)

    def test_a_lone_singleton_extrapolates_to_nothing(self) -> None:
        # f1 = 1, f2 = 0 → the bias-corrected form is f1·(f1−1)/2 = 0. Good–Turing still reports
        # mass (a new shape is plausible); Chao1's lower bound simply cannot name a number.
        deficit = coverage_deficit({"a": 9, "b": 1})

        assert deficit.richness == 2.0
        assert deficit.unseen == 0.0
        assert deficit.unseen_mass == pytest.approx(0.1)

    def test_rejects_an_empty_table(self) -> None:
        with pytest.raises(ValueError, match="at least one observed feature"):
            coverage_deficit({})

    def test_rejects_a_zero_count(self) -> None:
        # A Counter can carry a zero after arithmetic; zero observations is not an observation.
        with pytest.raises(ValueError, match="count must be >= 1"):
            coverage_deficit(Counter({"a": 3, "b": 0}))

    def test_rejects_an_out_of_range_confidence(self) -> None:
        with pytest.raises(ValueError, match="confidence must be in"):
            coverage_deficit({"a": 1, "b": 1}, confidence=1.0)

    def test_rendering_states_both_halves(self) -> None:
        line = format_coverage_deficit(coverage_deficit({f"s{i}": 1 for i in range(4)} | {"x": 2}))

        assert "5 observed" in line
        assert "estimated reachable (Chao1, 95% CI" in line
        assert "of seeds still discovering" in line

    def test_the_richness_is_never_rendered_as_a_bound(self) -> None:
        # Chao1's *estimand* is a lower bound on richness; the estimate is not. Sampled below
        # saturation it lands above the truth about as often as below (see the test beneath), so a
        # `≥` in front of it would claim a floor the number cannot carry — the exact shape of
        # overclaim this module exists to remove.
        line = format_coverage_deficit(coverage_deficit({f"s{i}": 1 for i in range(4)} | {"x": 2}))

        assert "≥" not in line
        assert "~" in line

    def test_the_point_estimate_overshoots_the_truth_about_half_the_time(self) -> None:
        # The measurement behind the rule above, so a future reader does not reinstate the `≥`.
        truth = 80

        def sample(seed: int) -> float:
            rng = random.Random(seed)  # one generator per sample, not one per draw
            return coverage_deficit(Counter(rng.randrange(truth) for _ in range(150))).richness

        overshoots = sum(sample(seed) > truth for seed in range(200))

        assert 60 < overshoots < 140, f"{overshoots}/200 — re-derive before trusting a bound glyph"

    def test_a_tiny_unseen_mass_never_renders_as_zero(self) -> None:
        # 0.0% reads as "saturated" when the sample is still discovering. The module already solves
        # this for bounds; the saturation line must agree on what "too small to show" looks like.
        # One singleton in a large pool is reachable from an audit() sweep, not a 200-seed one.
        deficit = coverage_deficit({"lonely": 1} | {f"s{i}": 3 for i in range(700)})

        assert 0.0 < deficit.unseen_mass < 0.0005
        assert "0.0%" not in format_coverage_deficit(deficit)
        assert "e-04" in format_coverage_deficit(deficit)

    def test_an_exact_zero_mass_still_renders_as_a_percentage(self) -> None:
        # The scientific fallback is for a mass too small to show, not for one that is genuinely
        # nothing — "0.0e+00 of seeds still discovering" is noise where "0.00%" is the answer.
        deficit = coverage_deficit({"a": 100, "b": 100, "c": 100})

        assert deficit.unseen_mass == 0.0
        assert "0.00% of seeds still discovering" in format_coverage_deficit(deficit)
        assert "e+00" not in format_coverage_deficit(deficit)


# ....................... #


class TestFamilywiseLevel:
    """Family-wise control, in both directions — a scan of m cells, and a family of K sweeps."""

    def test_hand_computed_level_for_a_fixed_m(self) -> None:
        # The union bound, γ' = 1 − α/m: the per-comparison level that leaves 5% family-wise error
        # across 15 cells. By hand: 1 − 0.05/15 = 1 − 0.003333… = 0.996666…
        assert familywise_level(0.95, 15) == pytest.approx(1.0 - 0.05 / 15)
        assert familywise_level(0.95, 15) == pytest.approx(0.9966667, abs=1e-7)

    def test_it_needs_no_independence_assumption(self) -> None:
        # The load-bearing property, and the reason it is not Šidák. Šidák's γ^(1/m) is tighter but
        # valid only under independence, and *anti-conservative* under positive dependence — which
        # is what a session sharing one --dst-seeds range across sweeps produces. The union bound
        # holds under arbitrary dependence, so it is never the looser-looking of the two by
        # accident: it is looser on purpose, and correct without a premise nothing here checks.
        for m in (2, 5, 15, 31, 60):
            sidak = 0.95 ** (1 / m)

            assert familywise_level(0.95, m) >= sidak  # never claims more than Šidák would
            assert familywise_level(0.95, m) - sidak < 1e-3  # and the price is negligible

    def test_one_comparison_is_the_uncorrected_level(self) -> None:
        assert familywise_level(0.95, 1) == pytest.approx(0.95)

    def test_more_comparisons_demand_a_stricter_per_cell_level(self) -> None:
        assert (
            familywise_level(0.95, 5) < familywise_level(0.95, 30) < familywise_level(0.95, 60) < 1.0
        )

    def test_the_family_wise_error_it_buys(self) -> None:
        # What the correction is for: uncorrected, m statements at 95% each leave a spurious flag
        # likelier than not by 15 comparisons.
        assert pytest.approx(0.2262, abs=5e-4) == 1.0 - 0.95**5
        assert pytest.approx(0.5367, abs=5e-4) == 1.0 - 0.95**15
        assert pytest.approx(0.9539, abs=5e-4) == 1.0 - 0.95**60

    def test_rejects_a_nonpositive_comparison_count(self) -> None:
        with pytest.raises(ValueError, match="comparisons must be >= 1"):
            familywise_level(0.95, 0)

    def test_rejects_an_out_of_range_confidence(self) -> None:
        with pytest.raises(ValueError, match="confidence must be in"):
            familywise_level(1.0, 10)


# ....................... #


class TestFormatFamilyVerdict:
    def test_the_simultaneous_bound_is_wider_than_any_per_sweep_one(self) -> None:
        per_sweep = detection_upper_bound(1000)
        family = format_family_verdict([1000] * 10)

        # 10 sweeps × 1000 seeds: 0.299% per sweep → 0.526% simultaneously (1.76× wider).
        assert "0 violations across 10 sweeps" in family
        assert "0.53%" in family
        assert "simultaneously" in family
        assert "(95% family-wise, Bonferroni over 10)" in family
        assert per_sweep < 0.0053

    def test_the_widest_corrected_bound_is_what_holds_for_all(self) -> None:
        # A mixed-size family is bounded by its smallest sweep — claiming the tightest would be
        # false of the others.
        expected = detection_upper_bound(50, confidence=familywise_level(0.95, 3))

        assert _render_probability(expected) in format_family_verdict([1000, 50, 400])

    def test_a_single_sweep_family_is_just_that_sweep(self) -> None:
        assert _render_probability(detection_upper_bound(200)) in format_family_verdict([200])

    def test_rejects_an_empty_family(self) -> None:
        with pytest.raises(ValueError, match="at least one sweep"):
            format_family_verdict([])


# ....................... #


class TestFlipMargin:
    """The exact sensitivity of a bound comparison to the one input carrying no interval."""

    @staticmethod
    def _respected(trigger: float, *, upper: float = 0.075, bound: float = 0.5) -> bool:
        """The scan's own verdict, restated: respect iff the conditional upper clears the bound."""

        return min(1.0, upper / trigger) >= bound

    def test_the_factor_is_the_exact_boundary(self) -> None:
        upper, bound, trigger = 0.075, 0.5, 0.0625
        margin = flip_margin(observed_upper=upper, bound=bound, trigger=trigger)

        assert margin.reachable
        assert margin.factor == pytest.approx((upper / bound) / trigger)

        # Just under F keeps the verdict; just over flips it. This is the whole claim.
        assert self._respected(trigger * margin.factor * 0.999)
        assert not self._respected(trigger * margin.factor * 1.001)

    def test_an_unreachable_flip_is_reported_as_such(self) -> None:
        # p̂_upper / bound > 1 → no admissible p_trigger flips this cell, and a numeric factor
        # would imply a probability above 1.
        margin = flip_margin(observed_upper=0.9, bound=0.25, trigger=0.5)

        assert not margin.reachable
        assert margin.factor * 0.5 > 1.0  # exactly why it cannot be quoted as a factor

    def test_the_boundary_case_of_exactly_one_is_still_reachable(self) -> None:
        # p̂_upper / bound == 1 lands on p_trigger = 1, which is admissible.
        margin = flip_margin(observed_upper=0.5, bound=0.5, trigger=0.25)

        assert margin.reachable
        assert margin.factor == pytest.approx(4.0)

    def test_a_violating_cell_reports_a_factor_below_one(self) -> None:
        # Below 1 the reading inverts: how far the constant would have to be *overstated* for the
        # violation to disappear.
        margin = flip_margin(observed_upper=0.01, bound=0.5, trigger=0.5)

        assert margin.factor < 1.0
        assert not self._respected(0.5, upper=0.01)

    def test_a_wide_margin_is_immune_to_any_plausible_derivation_error(self) -> None:
        assert flip_margin(observed_upper=0.02, bound=0.0002, trigger=0.5).factor > 40.0

    @pytest.mark.parametrize(
        ("kwargs", "match"),
        [
            ({"observed_upper": 1.5, "bound": 0.5, "trigger": 0.5}, "observed_upper must be"),
            ({"observed_upper": 0.5, "bound": 0.0, "trigger": 0.5}, "bound must be"),
            ({"observed_upper": 0.5, "bound": 0.5, "trigger": 0.0}, "trigger must be"),
        ],
    )
    def test_rejects_inadmissible_inputs(self, kwargs: dict[str, float], match: str) -> None:
        with pytest.raises(ValueError, match=match):
            flip_margin(**kwargs)  # type: ignore[arg-type]


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


class TestFisherExact:
    def test_lady_tasting_tea(self) -> None:
        # Fisher's own worked example: margins 4/4, C(8,4)=70; the two-sided sum-of-small-p
        # value for the 3-correct table is (1 + 16 + 16 + 1) / 70.
        assert fisher_exact(((3, 1), (1, 3))) == pytest.approx(34 / 70)

    def test_perfect_separation(self) -> None:
        # Only the observed table and its mirror are as improbable: 2 / C(20, 10).
        assert fisher_exact(((10, 0), (0, 10))) == pytest.approx(2 / 184756)

    def test_transpose_and_swap_invariance(self) -> None:
        p = fisher_exact(((1, 9), (11, 3)))

        assert fisher_exact(((1, 11), (9, 3))) == pytest.approx(p)  # transpose
        assert fisher_exact(((11, 3), (1, 9))) == pytest.approx(p)  # row swap
        assert p < 0.005  # strongly associated table

    def test_empty_margin_carries_no_evidence(self) -> None:
        # The predictor analysis's degenerate branches: no divergent cells, or no divergent
        # outcomes — either empty margin must report 1.0, never a spurious signal.
        assert fisher_exact(((12, 0), (0, 0))) == 1.0
        assert fisher_exact(((7, 5), (0, 0))) == 1.0
        assert fisher_exact(((7, 0), (5, 0))) == 1.0
        assert fisher_exact(((0, 0), (0, 0))) == 1.0

    def test_independent_table_is_near_one(self) -> None:
        assert fisher_exact(((5, 5), (5, 5))) == 1.0

    def test_validation(self) -> None:
        with pytest.raises(ValueError, match="must be >= 0"):
            fisher_exact(((1, -1), (2, 3)))


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


# ....................... #


class TestValidationBranches:
    """The refuse-loudly edges: every public entrypoint rejects malformed statistics inputs."""

    def test_binomial_ci_rejects_bad_confidence(self) -> None:
        with pytest.raises(ValueError, match="confidence"):
            binomial_ci(1, 10, confidence=1.0)

    def test_fit_rejects_bad_confidence(self) -> None:
        with pytest.raises(ValueError, match="confidence"):
            SurvivalCurve.fit([1], [], confidence=0.0)

    def test_quantile_rejects_out_of_range(self) -> None:
        curve = SurvivalCurve.fit([1, 2, 3], [])

        with pytest.raises(ValueError, match="q must be"):
            curve.quantile(1.0)

    def test_geometric_rejects_sub_one_times(self) -> None:
        with pytest.raises(ValueError, match=">= 1"):
            geometric_p_hat([0], [])


class TestKernelEdges:
    def test_betainc_saturates_at_the_support_edges(self) -> None:
        from forze_dst.stats import _betainc

        assert _betainc(2.0, 3.0, 0.0) == 0.0
        assert _betainc(2.0, 3.0, 1.0) == 1.0

    def test_fit_reaches_zero_survival_when_the_last_at_risk_all_detect(self) -> None:
        # Final step: at_risk == detected — survival hits 0 and the Greenwood increment is
        # skipped (its denominator would vanish); the band collapses with the curve.
        curve = SurvivalCurve.fit([1, 1], [])

        assert curve.steps[-1].survival == 0.0

    def test_log_rank_degenerates_to_no_evidence_on_zero_variance(self) -> None:
        # B leaves the risk set before A's only event: the sole event time has one subject at
        # risk, contributing no variance — the statistic is 0 and p is 1 by convention.
        result = log_rank([2], [], [], [1])

        assert result.statistic == 0.0
        assert result.p_value == 1.0
