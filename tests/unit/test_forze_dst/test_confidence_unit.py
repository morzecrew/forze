"""Unit coverage for the confidence report — formatting and the fold accumulators."""

from __future__ import annotations

from forze_dst.oracle.confidence import (
    ConfidenceProbe,
    ConfidenceReport,
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
