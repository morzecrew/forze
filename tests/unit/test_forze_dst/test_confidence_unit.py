"""Unit coverage for the confidence report — formatting and the fold accumulators."""

from __future__ import annotations

from forze_dst.faults import FaultPolicy, FaultRule
from forze_dst.oracle.confidence import (
    ConfidenceProbe,
    ConfidenceReport,
    _fault_label,
    assess_confidence,
)
from forze_dst.oracle.recorder import Event, History

# ----------------------- #


class TestConfidenceReportFormat:
    def test_clean_report(self) -> None:
        report = ConfidenceReport(seeds_run=10, ran_ops=("a",), raced_ops=("a",))

        out = report.format()

        assert "seeds run:" in out
        assert "every operation raced" in out
        assert report.clean

    def test_report_with_gaps_and_faults(self) -> None:
        report = ConfidenceReport(
            seeds_run=5,
            ran_ops=("a", "b"),
            raced_ops=("a",),
            faults_declared=("db[*].* (error)",),
            faults_fired=(),
        )

        out = report.format()

        assert "confidence gaps" in out
        assert "faults fired" in out
        assert report.never_raced == ("b",)
        assert report.faults_never_fired == ("db[*].* (error)",)
        assert not report.clean


# ....................... #


class TestProbe:
    def test_observe_skips_non_fault_timeline_event(self) -> None:
        history = History(
            seed=0,
            events=(
                Event(seq=0, kind="partition", at=0.0, fields={"node": "n1", "loss": 1.0}),
            ),
        )

        probe = ConfidenceProbe()
        probe.observe(history)
        report = probe.report()

        assert report.seeds_run == 1

    def test_assess_confidence_empty(self) -> None:
        report = assess_confidence([])

        assert report.seeds_run == 0
        assert report.ran_ops == ()
        assert report.clean

    def test_assess_confidence_folds_histories(self) -> None:
        history = History(
            seed=0,
            events=(
                Event(seq=0, kind="fault", at=0.0, fields={"surface": "db", "op": "get"}),
            ),
        )

        report = assess_confidence([history])

        assert report.seeds_run == 1


# ....................... #


class TestNeverRaced:
    """Operations that ran but never overlapped another are confidence gaps."""

    def test_op_that_never_raced_is_flagged(self) -> None:
        report = ConfidenceReport(seeds_run=2, ran_ops=("a", "b"), raced_ops=("a",))

        assert report.never_raced == ("b",)

    def test_all_ops_raced_leaves_no_gap(self) -> None:
        report = ConfidenceReport(seeds_run=2, ran_ops=("a", "b"), raced_ops=("a", "b"))

        assert report.never_raced == ()
        assert report.warnings == ()
        assert report.clean

    def test_no_ops_at_all_is_clean(self) -> None:
        report = ConfidenceReport(seeds_run=0, ran_ops=(), raced_ops=())

        assert report.never_raced == ()
        assert report.clean


# ....................... #


class TestFaultsNeverFired:
    """Declared fault rules no seed triggered are confidence gaps."""

    def test_declared_but_unfired_fault_is_flagged(self) -> None:
        report = ConfidenceReport(
            seeds_run=3,
            ran_ops=("a",),
            raced_ops=("a",),
            faults_declared=("db[*].* (error)", "cache[*].* (timeout)"),
            faults_fired=("db[*].* (error)",),
        )

        assert report.faults_never_fired == ("cache[*].* (timeout)",)
        assert not report.clean
        assert any("never fired" in w for w in report.warnings)

    def test_all_declared_faults_fired_leaves_no_gap(self) -> None:
        report = ConfidenceReport(
            seeds_run=3,
            ran_ops=("a",),
            raced_ops=("a",),
            faults_declared=("db[*].* (error)",),
            faults_fired=("db[*].* (error)",),
        )

        assert report.faults_never_fired == ()
        assert report.clean


# ....................... #


class TestWarningsAndClean:
    """Each gap becomes exactly one warning line; a gapless report is clean."""

    def test_both_gaps_yield_two_warnings(self) -> None:
        report = ConfidenceReport(
            seeds_run=4,
            ran_ops=("a", "b"),
            raced_ops=("a",),
            faults_declared=("db[*].* (error)",),
            faults_fired=(),
        )

        warnings = report.warnings

        assert len(warnings) == 2
        assert any("never raced" in w for w in warnings)
        assert any("never fired" in w for w in warnings)
        assert not report.clean

    def test_clean_report_has_no_warnings(self) -> None:
        report = ConfidenceReport(seeds_run=1, ran_ops=("a",), raced_ops=("a",))

        assert report.warnings == ()
        assert report.clean


# ....................... #


class TestFormatBranches:
    """format() renders the faults line only when faults were declared, and switches on gaps."""

    def test_clean_format_has_success_line_and_no_faults_line(self) -> None:
        report = ConfidenceReport(seeds_run=7, ran_ops=("a",), raced_ops=("a",))

        out = report.format()

        assert "DST confidence" in out
        assert "seeds run:    7" in out
        assert "1/1 operations overlapped" in out
        assert "✓ every operation raced and every declared fault fired" in out
        # No faults declared → no "faults fired:" line.
        assert "faults fired:" not in out
        assert "confidence gaps" not in out

    def test_format_with_faults_and_gaps(self) -> None:
        report = ConfidenceReport(
            seeds_run=5,
            ran_ops=("a", "b"),
            raced_ops=("a",),
            faults_declared=("db[*].* (error)",),
            faults_fired=(),
        )

        out = report.format()

        assert "faults fired: 0/1 declared rules" in out
        assert "⚠ confidence gaps:" in out
        assert "never raced" in out
        assert "never fired" in out
        assert "every operation raced" not in out


# ....................... #


class TestFaultLabel:
    """The human-readable label for a declared fault rule — selector + active kinds."""

    def test_label_includes_selector_and_kinds(self) -> None:
        rule = FaultRule(surface="db", route="orders", op="get", error=1.0, timeout=0.5)

        assert _fault_label(rule) == "db[orders].get (error/timeout)"

    def test_wildcards_render_as_star(self) -> None:
        # All selector parts None → every position is a wildcard.
        rule = FaultRule(error=1.0)

        assert _fault_label(rule) == "*[*].* (error)"

    def test_rule_with_no_active_kind_omits_the_kinds_suffix(self) -> None:
        # A rule with every probability at 0.0 has no active kinds → bare selector.
        rule = FaultRule(surface="db", op="get")

        assert _fault_label(rule) == "db[*].get"


# ....................... #


class TestProbeReportWithFaults:
    """ConfidenceProbe.report names declared rules and which of them actually fired."""

    def test_declared_fault_fires_when_matching_call_was_injected(self) -> None:
        fired = History(
            seed=0,
            events=(
                Event(seq=0, kind="fault", at=0.0,
                      fields={"surface": "db", "route": None, "op": "get"}),
            ),
        )

        probe = ConfidenceProbe()
        probe.observe(fired)
        policy = FaultPolicy(
            rules=(
                FaultRule(surface="db", error=1.0),        # matches the injected call
                FaultRule(surface="cache", timeout=1.0),   # never injected
            )
        )
        report = probe.report(faults=policy)

        assert len(report.faults_declared) == 2
        assert report.faults_fired == ("db[*].* (error)",)
        assert report.faults_never_fired == ("cache[*].* (timeout)",)
        assert not report.clean

    def test_report_without_faults_declares_nothing(self) -> None:
        probe = ConfidenceProbe()
        report = probe.report()

        assert report.faults_declared == ()
        assert report.faults_fired == ()
