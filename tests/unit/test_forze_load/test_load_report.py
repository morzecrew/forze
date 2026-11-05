"""Run files read back, ``--fail-on`` parsing, and the step-by-step comparison."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from forze.base.exceptions import CoreException
from forze_load import compare_runs, parse_fail_on, read_run, summarize
from forze_load.report import SCHEMA, RunFile

pytestmark = pytest.mark.unit

# ----------------------- #


def _row(workers: int, goodput: float, p95: float, **kw: Any) -> dict[str, Any]:
    return {
        "kind": "step",
        "workers": workers,
        "goodput_rps": goodput,
        "ms": {"p50": p95 / 2, "p95": p95, "p99": p95 * 1.5},
        "outcomes": {"server_error": 0, "transport": kw.get("transport", 0)},
        "misconfigured": False,
        "client": {"saturated": kw.get("saturated", False)},
    }


def _run(*rows: dict[str, Any], target: str = "http://a") -> RunFile:
    meta = {"kind": "meta", "schema": SCHEMA, "target": target, "model": "closed"}

    return RunFile(meta=meta, rows=list(rows), summary=None)


# ....................... #


class TestReadRun:
    def test_it_refuses_a_file_without_meta(self, tmp_path: Path) -> None:
        path = tmp_path / "x.jsonl"
        path.write_text(json.dumps({"kind": "step"}) + "\n")

        with pytest.raises(CoreException) as error:
            read_run(path)

        assert error.value.code == "load_run_file_invalid"

    def test_it_refuses_another_schema(self, tmp_path: Path) -> None:
        path = tmp_path / "x.jsonl"
        path.write_text(json.dumps({"kind": "meta", "schema": 99}) + "\n")

        with pytest.raises(CoreException, match="schema 99"):
            read_run(path)

    @pytest.mark.parametrize(
        "content",
        [
            "{not json\n",
            json.dumps({"kind": "meta", "schema": SCHEMA}) + "\n[1, 2]\n",
            json.dumps({"kind": "meta", "schema": SCHEMA})
            + "\n"
            + json.dumps({"kind": "step", "workers": 1})
            + "\n",
        ],
    )
    def test_it_refuses_malformed_lines(self, tmp_path: Path, content: str) -> None:
        path = tmp_path / "x.jsonl"
        path.write_text(content)

        with pytest.raises(CoreException) as error:
            read_run(path)

        assert error.value.code == "load_run_file_invalid"

    def test_it_refuses_a_missing_file(self, tmp_path: Path) -> None:
        with pytest.raises(CoreException) as error:
            read_run(tmp_path / "absent.jsonl")

        assert error.value.code == "load_run_file_invalid"

    def test_an_interrupted_run_has_no_summary(self, tmp_path: Path) -> None:
        path = tmp_path / "x.jsonl"
        lines = [{"kind": "meta", "schema": SCHEMA}, _row(1, 10, 5)]
        path.write_text("\n".join(json.dumps(line) for line in lines) + "\n\n")

        run = read_run(path)

        assert run.summary is None
        assert len(run.rows) == 1


class TestParseFailOn:
    def test_it_parses_percentages(self) -> None:
        assert parse_fail_on("p95:15%, goodput:10") == {"p95": 0.15, "goodput": 0.10}

    @pytest.mark.parametrize("spec", ["p42:10%", "p95", "p95:abc", "p95:-5%", "goodput:0"])
    def test_it_refuses_bad_specs(self, spec: str) -> None:
        with pytest.raises(CoreException) as error:
            parse_fail_on(spec)

        assert error.value.code == "load_fail_on_invalid"


class TestCompare:
    def test_it_reports_without_failing_by_default(self) -> None:
        comparison = compare_runs(_run(_row(8, 100, 50)), _run(_row(8, 50, 200)))

        assert comparison.failures == []
        assert comparison.lines[0].startswith("workers=8: goodput 100 -> 50 (worse 50.0%)")

    def test_thresholds_fail_only_the_metrics_named(self) -> None:
        comparison = compare_runs(
            _run(_row(8, 100, 50)),
            _run(_row(8, 95, 60)),
            fail_on={"p95": 0.15, "goodput": 0.10},
        )

        assert comparison.failures == ["workers=8: p95 worse by 20.0% (limit 15%)"]

    def test_better_is_never_a_failure(self) -> None:
        comparison = compare_runs(
            _run(_row(8, 100, 50)), _run(_row(8, 120, 40)), fail_on={"p95": 0.01}
        )

        assert comparison.failures == []
        assert "better" in comparison.lines[0]

    def test_differences_in_target_and_steps_are_warnings(self) -> None:
        comparison = compare_runs(
            _run(_row(1, 10, 5), _row(2, 20, 5)),
            _run(_row(1, 10, 5), _row(4, 40, 5), target="http://b"),
        )

        assert "runs differ in target: comparing anyway" in comparison.warnings
        assert "workers=2: only in one run" in comparison.warnings
        assert "workers=4: only in one run" in comparison.warnings

    def test_different_weights_or_mixes_are_warned(self) -> None:
        base, head = _run(_row(1, 10, 5)), _run(_row(1, 10, 5))
        base.meta["plan"] = {"principals": {"a": 1}, "mixes": {"a": {"x": 1}}, "seed": 0}
        head.meta["plan"] = {"principals": {"a": 2}, "mixes": {"a": {"x": 1, "y": 1}}, "seed": 1}

        warnings = compare_runs(base, head).warnings

        assert (
            "runs differ in principal weights: a different mix is a different workload" in warnings
        )
        assert "runs differ in principal mixes: a different mix is a different workload" in warnings
        assert "runs differ in plan (scenarios, timeout or seed): comparing anyway" in warnings

    def test_equal_rates_match_whatever_their_number_type(self) -> None:
        base, head = _row(1, 10, 5), _row(1, 10, 5)
        del base["workers"], head["workers"]
        base["rate"], head["rate"] = 50, 50.0

        comparison = compare_runs(_run(base), _run(head))

        assert len(comparison.lines) == 1
        assert comparison.lines[0].startswith("rate=50:")
        assert comparison.warnings == []

    def test_a_repeated_step_is_compared_by_its_position(self) -> None:
        base = _run(_row(1, 10, 5), _row(2, 20, 5), _row(2, 21, 5))
        head = _run(_row(1, 10, 5), _row(2, 20, 5), _row(2, 10, 5))

        comparison = compare_runs(base, head, fail_on={"goodput": 0.10})

        assert len(comparison.lines) == 3
        assert comparison.failures == ["workers=2 (2nd): goodput worse by 52.4% (limit 10%)"]

    def test_missing_latency_compares_as_unknown(self) -> None:
        row = _row(1, 0, 5)
        row["ms"] = {"p50": None, "p95": None, "p99": None}

        comparison = compare_runs(_run(row), _run(_row(1, 10, 5)), fail_on={"goodput": 0.1})

        assert comparison.failures == []
        assert "p95 - -> 5" in comparison.lines[0]


def test_summarize_names_saturation_and_the_knee() -> None:
    rows = [_row(1, 10, 5), _row(2, 12, 6, saturated=True)]
    lines = summarize(rows, {"knee": None, "note": "client saturated", "stopped": {"x": 1}})

    assert "client saturated" in lines[1]
    assert lines[2] == "knee: none (client saturated)"
    assert lines[3].startswith("stopped:")
