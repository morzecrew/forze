"""Per-invariant at-risk denominators — the verdict's clause stops dividing by one shared ``S``.

The countable oracle-set clause names ``N`` witnessed invariants and quotes one bound derived from
``S`` for all of them. But each invariant was only *at risk* in some ``n_i <= S`` runs, and the
bound for ``n_i`` can be an order of magnitude weaker: at ``S = 1000`` the aggregate reads
``< 0.30%`` while an invariant exposed on 50 runs is really only bounded at ``< 5.82%``.

Vacuity already caught the binary edge of this (``n_i == 0``). These tests cover everything in
between: the counts are real, the weakest member is named beside the aggregate, and an invariant
whose read footprint is opaque is reported as unmeasured rather than quietly folded in at
``n = S``.
"""

from __future__ import annotations

import pytest

from forze_dst.oracle.confidence import ConfidenceProbe, ConfidenceReport
from forze_dst.oracle.horizon import HorizonProbe
from forze_dst.oracle.invariants import Invariant, Violation, named
from forze_dst.oracle.recorder import Event, History
from forze_dst.stats import detection_upper_bound, format_clean_verdict

# ----------------------- #


def _ev(seq: int, kind: str, **fields: object) -> Event:
    return Event(seq=seq, kind=kind, at=float(seq), fields=fields)


def _reads(name: str, kind: str) -> Invariant:
    """An invariant whose read footprint is exactly *kind*. Real, but never fires here — these
    tests are about denominators, not detections, and no event below carries ``boom``."""

    def _check(history: History) -> list[Violation]:
        return [
            Violation(invariant=name, message="boom", events=(event,))
            for event in history.of_kind(kind)
            if event.fields.get("boom")
        ]

    return named(name, _check)


def _opaque(name: str) -> Invariant:
    """An invariant that iterates ``history.events`` wholesale — footprint unknowable."""

    def _check(history: History) -> list[Violation]:
        return [
            Violation(invariant=name, message="boom", events=(event,))
            for event in history.events
            if event.fields.get("boom")
        ]

    return named(name, _check)


def _run(seed: int, *kinds: str) -> History:
    """A history recording exactly *kinds* (plus the machinery kind every run carries)."""

    return History(
        seed=seed,
        events=(
            _ev(0, "operation", op="go", outcome="ok"),
            *(_ev(i + 1, kind) for i, kind in enumerate(kinds)),
        ),
    )


# ....................... #


class TestAtRiskCounts:
    """One invariant exercised on every run, one on a known small subset — two denominators."""

    @staticmethod
    def _probe(runs: int, rare_in: int) -> HorizonProbe:
        probe = HorizonProbe(
            invariants=[_reads("always_at_risk", "common"), _reads("rarely_at_risk", "rare")]
        )
        for seed in range(runs):
            probe.observe(_run(seed, "common", *(("rare",) if seed < rare_in else ())))
        return probe

    def test_the_counts_are_per_invariant_not_the_sweep_size(self) -> None:
        analysis = self._probe(runs=40, rare_in=5).analyze(
            [_reads("always_at_risk", "common"), _reads("rarely_at_risk", "rare")]
        )

        assert analysis.runs == 40
        assert dict(analysis.at_risk) == {"always_at_risk": 40, "rarely_at_risk": 5}

    def test_the_two_invariants_get_different_bounds(self) -> None:
        analysis = self._probe(runs=40, rare_in=5).analyze(
            [_reads("always_at_risk", "common"), _reads("rarely_at_risk", "rare")]
        )
        exposures = dict(analysis.at_risk)

        wide = detection_upper_bound(exposures["always_at_risk"])
        narrow = detection_upper_bound(exposures["rarely_at_risk"])

        # Not a rounding difference — 5 runs licenses an order of magnitude less than 40.
        assert narrow > wide * 5

    def test_a_run_is_never_counted_twice_for_one_name(self) -> None:
        # Duplicate names would otherwise double-increment and produce n_i > runs, an exposure
        # count that overstates rather than understates. The accounting gate forbids duplicates;
        # the probe must not depend on that to stay sound.
        probe = HorizonProbe(invariants=[_reads("dup", "a"), _reads("dup", "b")])
        for seed in range(10):
            probe.observe(_run(seed, "a", "b"))

        analysis = probe.analyze([_reads("dup", "a")])

        assert dict(analysis.at_risk) == {"dup": 10}

    def test_vacuity_is_the_zero_edge_of_the_same_measurement(self) -> None:
        probe = HorizonProbe(invariants=[_reads("never_at_risk", "absent")])
        for seed in range(10):
            probe.observe(_run(seed, "common"))

        analysis = probe.analyze([_reads("never_at_risk", "absent")])

        assert dict(analysis.at_risk) == {"never_at_risk": 0}
        assert [name for name, _ in analysis.vacuous] == ["never_at_risk"]


# ....................... #


class TestOpaqueFootprintsAreUnmeasured:
    def test_an_opaque_invariant_is_named_not_folded_in_at_full_exposure(self) -> None:
        probe = HorizonProbe(invariants=[_opaque("reads_everything"), _reads("narrow", "rare")])
        for seed in range(20):
            probe.observe(_run(seed, "rare") if seed < 3 else _run(seed, "common"))

        analysis = probe.analyze([_opaque("reads_everything"), _reads("narrow", "rare")])

        assert analysis.unmeasured_exposure == ("reads_everything",)
        # The load-bearing half: it is absent from the counts, not present at n = runs.
        assert dict(analysis.at_risk) == {"narrow": 3}
        assert "reads_everything" not in dict(analysis.at_risk)

    def test_an_invariant_analyzed_but_never_declared_to_the_probe_is_unmeasured(self) -> None:
        # Exposure is a per-history fact. A probe that was not told about an invariant up front
        # cannot recover it afterwards, and must say so rather than assume the full sweep.
        probe = HorizonProbe()
        for seed in range(10):
            probe.observe(_run(seed, "common"))

        analysis = probe.analyze([_reads("late", "common")])

        assert analysis.unmeasured_exposure == ("late",)
        assert analysis.at_risk == ()


# ....................... #


class TestTheVerdictCarriesTheWeakestMember:
    @staticmethod
    def _report(**overrides: object) -> ConfidenceReport:
        base: dict[str, object] = {
            "seeds_run": 1000,
            "ran_ops": ("go",),
            "raced_ops": ("go",),
            "at_risk_runs": (("broad", 1000), ("narrow", 50)),
        }
        base.update(overrides)
        return ConfidenceReport(**base)  # type: ignore[arg-type]

    def test_the_aggregate_is_kept_and_the_weakest_is_named_beside_it(self) -> None:
        verdict = self._report().verdict()

        # The aggregate answers "did this sweep catch anything" and is what gets quoted, so it
        # stays — with the number it actually overstates printed directly under it.
        assert "0 violations in 1000 seeds → per-seed detection probability < 0.30%" in verdict
        assert "weakest coverage: narrow at risk in 50/1000 runs" in verdict
        assert "< 5.82% for that invariant alone" in verdict

    def test_the_weakest_member_is_the_narrowest_one(self) -> None:
        report = self._report(at_risk_runs=(("a", 900), ("b", 4), ("c", 250)))

        assert report.weakest_exposure == ("b", 4)
        assert "weakest coverage: b at risk in 4/1000 runs" in report.verdict()

    def test_no_line_when_every_invariant_was_at_risk_every_run(self) -> None:
        # Then the aggregate already *is* its own weakest member; the extra line would be noise.
        report = self._report(at_risk_runs=(("a", 1000), ("b", 1000)))

        assert report.weakest_exposure is None
        assert "weakest coverage" not in report.verdict()

    def test_a_never_at_risk_invariant_says_so_instead_of_quoting_a_bound(self) -> None:
        # detection_upper_bound(0) is undefined, and "at risk in 0 runs" is not a weak bound —
        # it is no bound. The sentence must not invent one.
        verdict = self._report(at_risk_runs=(("a", 1000), ("vacuous", 0))).verdict()

        assert "weakest coverage: vacuous at risk in 0/1000 runs" in verdict
        assert "does not cover it at all" in verdict
        assert "for that invariant alone" not in verdict

    def test_unmeasured_exposure_is_stated_in_the_verdict(self) -> None:
        verdict = self._report(unmeasured_exposure=("reads_everything",)).verdict()

        assert "unmeasured exposure: reads_everything" in verdict
        assert "opaque read footprint" in verdict

    def test_the_injected_regression_the_shared_S_would_fail_this(self) -> None:
        # Reverting to one denominator for every invariant is exactly the aggregate sentence,
        # and it carries neither of the clauses above.
        report = self._report(unmeasured_exposure=("opaque",))
        shared_s = format_clean_verdict(report.seeds_run)

        assert "weakest coverage" not in shared_s
        assert "unmeasured exposure" not in shared_s
        assert report.verdict() != shared_s

    def test_a_withheld_verdict_stays_withheld(self) -> None:
        # Exposure clauses must not smuggle a number back into a sweep that has no denominator.
        verdict = self._report(data_dependent_stop="plateau stop").verdict()

        assert "no per-seed bound" in verdict
        assert "%" not in verdict


# ....................... #


class TestScopeMatchesTheClauseTheVerdictPrints:
    def test_the_weakest_is_drawn_from_the_invariants_the_clause_names(self) -> None:
        # With accounting on, the sentence claims the *witnessed* set. Naming a weakest member
        # outside it would report a bound for something the clause never covered.
        from forze_dst.oracle.witness import InvariantAccounting, InvariantStatus

        accounting = InvariantAccounting(
            statuses=(
                ("witnessed_one", InvariantStatus.WITNESSED),
                ("declared_one", InvariantStatus.DECLARED),
            )
        )
        report = ConfidenceReport(
            seeds_run=100,
            ran_ops=("go",),
            raced_ops=("go",),
            accounting=accounting,
            at_risk_runs=(("witnessed_one", 60), ("declared_one", 2)),
        )

        assert report.weakest_exposure == ("witnessed_one", 60)
        assert "weakest coverage: witnessed_one" in report.verdict()


# ....................... #


class TestEndToEndThroughTheProbe:
    def test_the_confidence_probe_threads_exposure_into_the_report(self) -> None:
        probe = ConfidenceProbe(
            invariants=[_reads("broad", "common"), _reads("narrow", "rare")]
        )
        for seed in range(20):
            probe.observe(_run(seed, "common", *(("rare",) if seed < 2 else ())))

        report = probe.report(invariants=[_reads("broad", "common"), _reads("narrow", "rare")])

        assert dict(report.at_risk_runs) == {"broad": 20, "narrow": 2}
        assert report.weakest_exposure == ("narrow", 2)
        assert "weakest coverage: narrow at risk in 2/20 runs" in report.format()

    def test_no_invariants_means_no_exposure_claims_at_all(self) -> None:
        probe = ConfidenceProbe()
        probe.observe(_run(0, "common"))

        report = probe.report()

        assert report.at_risk_runs == ()
        assert report.unmeasured_exposure == ()
        assert report.weakest_exposure is None


# ....................... #


def test_detection_upper_bound_still_refuses_zero_runs() -> None:
    # The guard the never-at-risk branch exists to respect.
    with pytest.raises(ValueError, match="runs must be >= 1"):
        detection_upper_bound(0)


# ....................... #


class TestSeedRedundancy:
    """`S` seeds are treated as `S` independent chances by every bound. Measured on the corpus,
    200 seeds routinely explore one to three distinct execution shapes — a gap worth naming, and
    (deliberately) not worth substituting into the denominator."""

    @staticmethod
    def _report(seeds: int, shapes: int) -> ConfidenceReport:
        return ConfidenceReport(
            seeds_run=seeds,
            ran_ops=("go",),
            raced_ops=("go",),
            shape_counts={f"shape-{i}": 1 for i in range(shapes)},
        )

    def test_fires_on_a_low_diversity_sweep(self) -> None:
        # The corpus control's shape: 300 seeds, 2 distinct shapes.
        report = self._report(seeds=300, shapes=2)

        assert report.redundant_seeds
        assert any(
            "300 seeds explored 2 distinct execution shapes" in w for w in report.warnings
        )
        assert any("counts seeds, not distinct trials" in w for w in report.warnings)

    def test_stays_silent_on_a_high_diversity_sweep(self) -> None:
        # The deep mutant's shape: 200 seeds, 55 distinct shapes.
        report = self._report(seeds=200, shapes=55)

        assert not report.redundant_seeds
        assert not any("distinct execution shapes" in w for w in report.warnings)

    def test_the_corpus_spread_is_separated_cleanly_by_the_threshold(self) -> None:
        # Calibrated against the corpus's own range at 200 seeds (1, 2, 3, 3, 19, 55 shapes)
        # rather than a round number: the redundant end fires, the diverse end does not, and
        # neither sits near the boundary.
        redundant = [self._report(200, n).redundant_seeds for n in (1, 2, 3)]
        diverse = [self._report(200, n).redundant_seeds for n in (19, 55)]

        assert all(redundant)
        assert not any(diverse)

    def test_a_short_sweep_stays_quiet(self) -> None:
        # One shape in ten seeds is a short sweep, not evidence of redundancy.
        assert not self._report(seeds=10, shapes=1).redundant_seeds

    def test_the_bound_is_never_repriced_by_the_signal(self) -> None:
        # The guard against this warning quietly becoming a corrected denominator. The fingerprint
        # erases entity ids — the very dimension the collision pools vary — so distinct-shape
        # count is a coarse *lower* proxy for effective sample size, and substituting it would
        # trade a known overstatement for an unknown understatement.
        redundant = self._report(seeds=300, shapes=2)
        diverse = self._report(seeds=300, shapes=250)

        assert redundant.redundant_seeds and not diverse.redundant_seeds
        assert redundant.verdict() == diverse.verdict()
        assert redundant.verdict() == format_clean_verdict(300)

    def test_the_warning_is_absent_when_no_shapes_were_recorded(self) -> None:
        assert not ConfidenceReport(seeds_run=300, ran_ops=(), raced_ops=()).redundant_seeds


# ....................... #


class TestRedundancyEndToEnd:
    def test_a_single_shape_sweep_is_flagged_through_the_probe(self) -> None:
        probe = ConfidenceProbe()
        for seed in range(60):
            probe.observe(_run(seed, "common"))  # every run drives the identical shape

        report = probe.report()

        assert len(report.shape_counts) == 1
        assert report.redundant_seeds
        assert "60 seeds explored 1 distinct execution shapes" in "\n".join(report.warnings)

    def test_a_diverse_sweep_is_not(self) -> None:
        probe = ConfidenceProbe()
        for seed in range(60):
            # A different *execution shape* every run: the fingerprint folds operation outcomes
            # and port edges, so the ops are what has to vary — a marker kind would not move it.
            probe.observe(
                History(
                    seed=seed,
                    events=tuple(
                        _ev(i, "operation", op=f"op{i}", outcome="ok") for i in range(seed % 20 + 1)
                    ),
                )
            )

        report = probe.report()

        assert len(report.shape_counts) > 1
        assert not report.redundant_seeds
