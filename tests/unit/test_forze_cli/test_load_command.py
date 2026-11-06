"""``forze load run`` and ``forze load compare`` — resolution, refusals and exit codes."""

from __future__ import annotations

import json
import secrets
from pathlib import Path

import pytest
from typer.testing import CliRunner

from forze_cli.app import app
from forze_load.report import SCHEMA

pytestmark = pytest.mark.unit

_PLANS = "tests.unit.test_forze_load._load_plans"

runner = CliRunner()

# ....................... #


def _write_run(path: Path, goodput: float) -> Path:
    records = [
        {"kind": "meta", "schema": SCHEMA, "target": "http://a"},
        {
            "kind": "step",
            "workers": 1,
            "goodput_rps": goodput,
            "ms": {"p50": 1.0, "p95": 2.0, "p99": 3.0},
        },
    ]
    path.write_text("\n".join(json.dumps(record) for record in records) + "\n")

    return path


class TestRun:
    @pytest.mark.parametrize("ref", [f"{_PLANS}:plan", f"{_PLANS}:make_plan"])
    def test_it_runs_a_plan_and_writes_the_file(self, ref: str, tmp_path: Path) -> None:
        out = tmp_path / "run.jsonl"

        result = runner.invoke(
            app,
            [
                "load",
                "run",
                ref,
                "--steps",
                "1,2",
                "--step-s",
                "0.2",
                "--warm-s",
                "0",
                "--out",
                str(out),
            ],
        )

        assert result.exit_code == 0, result.output
        assert "workers=1: goodput" in result.output
        assert "knee:" in result.output
        assert len(out.read_text().splitlines()) == 4

    def test_the_open_model_runs(self, tmp_path: Path) -> None:
        result = runner.invoke(
            app,
            [
                "load",
                "run",
                f"{_PLANS}:plan",
                "--model",
                "open",
                "--steps",
                "20",
                "--step-s",
                "0.2",
                "--warm-s",
                "0",
                "--out",
                str(tmp_path / "o.jsonl"),
            ],
        )

        assert result.exit_code == 0, result.output
        assert "rate=20:" in result.output

    def test_a_writing_plan_is_refused_with_exit_2(self, tmp_path: Path) -> None:
        result = runner.invoke(
            app, ["load", "run", f"{_PLANS}:writing_plan", "--out", str(tmp_path / "w.jsonl")]
        )

        assert result.exit_code == 2
        assert "does not allow writes" in result.output

    @pytest.mark.parametrize(
        "ref", ["no-colon", "tests.unit.test_forze_load.absent_module:plan", f"{_PLANS}:absent"]
    )
    def test_a_reference_that_cannot_load_exits_2(self, ref: str) -> None:
        result = runner.invoke(app, ["load", "run", ref])

        assert result.exit_code == 2, result.output
        assert "cannot load" in result.output

    @pytest.mark.parametrize(
        "ref",
        [f"{_PLANS}:refusing_factory", "tests.unit.test_forze_load._refusing_plan_module:plan"],
    )
    def test_a_plan_refused_at_import_or_by_its_factory_exits_2(self, ref: str) -> None:
        result = runner.invoke(app, ["load", "run", ref])

        assert result.exit_code == 2, result.output
        assert "refused" in result.output

    def test_an_object_that_is_not_a_plan_is_refused(self) -> None:
        result = runner.invoke(app, ["load", "run", f"{_PLANS}:not_a_plan"])

        assert result.exit_code == 2
        assert "is not a LoadPlan" in result.output

    def test_an_unknown_model_is_a_usage_error(self) -> None:
        result = runner.invoke(app, ["load", "run", f"{_PLANS}:plan", "--model", "sideways"])

        assert result.exit_code == 2
        assert "closed" in result.output

    def test_a_ramp_that_stops_is_a_result_and_exits_0(self, tmp_path: Path) -> None:
        result = runner.invoke(
            app,
            [
                "load",
                "run",
                f"{_PLANS}:failing_plan",
                "--steps",
                "1,2",
                "--step-s",
                "0.2",
                "--warm-s",
                "0",
                "--out",
                str(tmp_path / "f.jsonl"),
            ],
        )

        assert result.exit_code == 0, result.output
        assert "stopped:" in result.output

    def test_a_read_sweep_lists_the_routes_it_skipped(self, tmp_path: Path) -> None:
        out = tmp_path / "s.jsonl"

        result = runner.invoke(
            app,
            ["load", "run", f"{_PLANS}:sweep_plan", "--steps", "1", "--step-s", "0.2"]
            + ["--warm-s", "0", "--out", str(out)],
        )

        assert result.exit_code == 0, result.output
        assert "skipped items.search: needs ['q']" in result.output

    def test_an_operation_whose_kind_disagrees_exits_2(self, tmp_path: Path) -> None:
        result = runner.invoke(
            app, ["load", "run", f"{_PLANS}:mismatch_plan", "--out", str(tmp_path / "m.jsonl")]
        )

        assert result.exit_code == 2, result.output
        assert "refused" in result.output
        assert "writes=False" in result.output

    def test_a_sign_in_transport_error_exits_2(self, tmp_path: Path, monkeypatch) -> None:
        monkeypatch.setenv("FORZE_LOAD_TEST_PASSWORD", secrets.token_hex(8))

        result = runner.invoke(
            app, ["load", "run", f"{_PLANS}:login_plan", "--out", str(tmp_path / "l.jsonl")]
        )

        assert result.exit_code == 2
        assert "transport error" in result.output

    def test_fractional_closed_steps_are_a_usage_error(self) -> None:
        result = runner.invoke(app, ["load", "run", f"{_PLANS}:plan", "--steps", "1,2.5"])

        assert result.exit_code == 2
        assert "whole worker counts" in result.output

    @pytest.mark.parametrize("steps", ["1,,4", "1,2,", ",", ""])
    def test_an_empty_step_is_a_usage_error(self, steps: str, tmp_path: Path) -> None:
        out = tmp_path / "e.jsonl"

        result = runner.invoke(
            app, ["load", "run", f"{_PLANS}:plan", "--steps", steps, "--out", str(out)]
        )

        assert result.exit_code == 2, result.output
        assert not out.exists()  # refused before anything ran

    @pytest.mark.parametrize("steps", ["nan", "inf", "10,nan"])
    def test_non_finite_steps_exit_2(self, steps: str) -> None:
        result = runner.invoke(
            app, ["load", "run", f"{_PLANS}:plan", "--model", "open", "--steps", steps]
        )

        assert result.exit_code == 2, result.output
        assert "finite" in result.output

    def test_bad_steps_are_a_usage_error(self) -> None:
        result = runner.invoke(app, ["load", "run", f"{_PLANS}:plan", "--steps", "1,x"])

        assert result.exit_code == 2


class TestCompare:
    def test_it_reports_and_exits_0_without_thresholds(self, tmp_path: Path) -> None:
        base = _write_run(tmp_path / "base.jsonl", 100)
        head = _write_run(tmp_path / "head.jsonl", 50)

        result = runner.invoke(app, ["load", "compare", str(base), str(head)])

        assert result.exit_code == 0, result.output
        assert "goodput 100 -> 50 (worse 50.0%)" in result.output

    def test_it_exits_1_past_a_threshold(self, tmp_path: Path) -> None:
        base = _write_run(tmp_path / "base.jsonl", 100)
        head = _write_run(tmp_path / "head.jsonl", 50)

        result = runner.invoke(
            app, ["load", "compare", str(base), str(head), "--fail-on", "goodput:10%"]
        )

        assert result.exit_code == 1
        assert "FAIL workers=1: goodput worse by 50.0%" in result.output

    @pytest.mark.parametrize("content", [None, "{broken\n", "[1]\n"])
    def test_an_unreadable_run_file_exits_2_never_1(
        self, tmp_path: Path, content: str | None
    ) -> None:
        base = _write_run(tmp_path / "base.jsonl", 100)
        head = tmp_path / "head.jsonl"

        if content is not None:
            head.write_text(content)

        result = runner.invoke(
            app, ["load", "compare", str(base), str(head), "--fail-on", "goodput:10%"]
        )

        assert result.exit_code == 2
        assert "refused" in result.output

    def test_a_gated_compare_against_a_run_with_no_step_exits_2(self, tmp_path: Path) -> None:
        base = _write_run(tmp_path / "base.jsonl", 100)
        head = tmp_path / "head.jsonl"
        head.write_text(json.dumps({"kind": "meta", "schema": SCHEMA, "target": "http://a"}) + "\n")

        gated = runner.invoke(
            app, ["load", "compare", str(base), str(head), "--fail-on", "goodput:10%"]
        )
        reported = runner.invoke(app, ["load", "compare", str(base), str(head)])

        assert gated.exit_code == 2, gated.output
        assert "refused" in gated.output
        assert reported.exit_code == 0, reported.output
        assert "nothing was compared" in reported.output

    def test_an_empty_threshold_is_refused_not_ignored(self, tmp_path: Path) -> None:
        base = _write_run(tmp_path / "base.jsonl", 100)

        result = runner.invoke(app, ["load", "compare", str(base), str(base), "--fail-on", ""])

        assert result.exit_code == 2, result.output
        assert "refused" in result.output

    def test_a_bad_threshold_exits_2(self, tmp_path: Path) -> None:
        base = _write_run(tmp_path / "base.jsonl", 100)

        result = runner.invoke(app, ["load", "compare", str(base), str(base), "--fail-on", "p7:1%"])

        assert result.exit_code == 2
        assert "refused" in result.output


def test_the_group_refuses_without_the_extra(monkeypatch) -> None:
    import forze_cli._compat as compat

    monkeypatch.setattr(compat, "find_spec", lambda _name: None)

    result = runner.invoke(app, ["load", "compare", "a", "b"])

    # 2, not 1: exit 1 means --fail-on was crossed, and a missing extra is no regression.
    assert result.exit_code == 2
    assert "forze[cli,load]" in result.output
