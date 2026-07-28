"""The quantitative clean-run verdict — exact zero-event bound + the one locked sentence.

A clean sweep must state what it excludes (per-seed detection probability bounded at a stated
confidence), never a bare "passed" — and a violating run must never print the bound at all.
"""

from types import MappingProxyType

import pytest

from forze_dst.artifacts.sweep import SweepResult
from forze_dst.oracle.confidence import ConfidenceReport, assess_confidence
from forze_dst.oracle.coverage import CoverageStats
from forze_dst.oracle.invariants import Violation
from forze_dst.oracle.recorder import History
from forze_dst.oracle.replay import ViolationReport
from forze_dst.stats import detection_upper_bound, format_clean_verdict

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

    def test_tiny_bound_falls_back_to_scientific_never_zero_percent(self) -> None:
        out = format_clean_verdict(10_000_000)

        assert "0.00%" not in out
        assert "e-07" in out


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
