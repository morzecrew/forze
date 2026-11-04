"""``forze load run`` and ``forze load compare`` — resolution, refusals and exit codes."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

pytest.importorskip("typer")
pytest.importorskip("httpx")

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
        assert "rate=20.0" in result.output

    def test_a_writing_plan_is_refused_with_exit_2(self, tmp_path: Path) -> None:
        result = runner.invoke(
            app, ["load", "run", f"{_PLANS}:writing_plan", "--out", str(tmp_path / "w.jsonl")]
        )

        assert result.exit_code == 2
        assert "does not allow writes" in result.output

    def test_an_object_that_is_not_a_plan_is_refused(self) -> None:
        result = runner.invoke(app, ["load", "run", f"{_PLANS}:not_a_plan"])

        assert result.exit_code == 2
        assert "is not a LoadPlan" in result.output

    def test_an_unknown_model_is_a_usage_error(self) -> None:
        result = runner.invoke(app, ["load", "run", f"{_PLANS}:plan", "--model", "sideways"])

        assert result.exit_code == 2
        assert "closed" in result.output

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

    def test_a_bad_threshold_exits_2(self, tmp_path: Path) -> None:
        base = _write_run(tmp_path / "base.jsonl", 100)

        result = runner.invoke(app, ["load", "compare", str(base), str(base), "--fail-on", "p7:1%"])

        assert result.exit_code == 2
        assert "refused" in result.output


def test_the_group_refuses_without_the_extra(monkeypatch) -> None:
    import forze_cli._compat as compat

    monkeypatch.setattr(compat, "find_spec", lambda _name: None)

    result = runner.invoke(app, ["load", "compare", "a", "b"])

    assert result.exit_code == 1
    assert "forze[cli,load]" in result.output
