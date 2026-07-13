"""Tests for the per-package coverage-floors checker (.github/scripts/coverage_floors.py).

The checker closes the "thin package hides behind the global number" gap: the
global ``fail_under`` gate is repo-wide, so a small new package can ship near-0%
covered while the total barely moves. These tests feed the checker synthetic
``coverage json`` payloads and verify the gate semantics:

- a below-floor package fails, and the failure names the package, its coverage,
  and its floor;
- the ``[tool.coverage_floors.exceptions]`` table is respected;
- an unknown (brand-new) package is gated at the default floor automatically;
- a src/ package with no coverage data at all fails (nothing escapes the gate);
- a stale exceptions entry (package no longer under src/) fails.

The real repo policy in ``pyproject.toml`` is also validated for shape so the
table cannot silently rot.
"""

from __future__ import annotations

import importlib.util
import json
import sys
import tomllib
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

# ----------------------- #

_REPO = Path(__file__).resolve().parents[2]
_SCRIPT = _REPO / ".github" / "scripts" / "coverage_floors.py"


def _load_checker() -> ModuleType:
    spec = importlib.util.spec_from_file_location("coverage_floors", _SCRIPT)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load checker script at {_SCRIPT}")
    module = importlib.util.module_from_spec(spec)
    # dataclass processing resolves the defining module via sys.modules, so the
    # script must be registered before exec, like a normal import would.
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


checker = _load_checker()


def _file_entry(covered: int, statements: int) -> dict[str, Any]:
    return {
        "summary": {
            "covered_lines": covered,
            "num_statements": statements,
            "covered_branches": 0,
            "num_branches": 0,
        }
    }


def _report(files: dict[str, dict[str, Any]]) -> dict[str, Any]:
    return {"files": files}


def _floors(default: float = 80.0, **exceptions: float) -> Any:
    return checker.Floors(default=default, exceptions=dict(exceptions))


# ----------------------- #
# Aggregation


def test_aggregates_lines_and_branches_per_package() -> None:
    report = _report(
        {
            "src/forze/a.py": {
                "summary": {
                    "covered_lines": 8,
                    "num_statements": 10,
                    "covered_branches": 3,
                    "num_branches": 5,
                }
            },
            "src/forze/sub/b.py": {
                "summary": {
                    "covered_lines": 4,
                    "num_statements": 5,
                    "covered_branches": 0,
                    "num_branches": 0,
                }
            },
        }
    )
    measured = checker.aggregate_packages(report)
    assert set(measured) == {"forze"}
    # (8 + 3 + 4) / (10 + 5 + 5) = 15/20 = 75%
    assert measured["forze"].percent == pytest.approx(75.0)


def test_non_src_files_are_ignored() -> None:
    report = _report(
        {
            "src/forze_x/a.py": _file_entry(10, 10),
            "tests/unit/test_a.py": _file_entry(0, 100),
        }
    )
    assert set(checker.aggregate_packages(report)) == {"forze_x"}


# ----------------------- #
# Gate semantics


def test_below_floor_package_fails_naming_package_coverage_and_floor() -> None:
    measured = checker.aggregate_packages(
        _report(
            {
                "src/forze_core/a.py": _file_entry(95, 100),
                "src/forze_thin/a.py": _file_entry(3, 100),
            }
        )
    )
    violations = checker.check_floors(
        measured, _floors(default=80.0), {"forze_core", "forze_thin"}
    )
    assert len(violations) == 1
    message = violations[0]
    assert "forze_thin" in message
    assert "3.0%" in message
    assert "80.0%" in message


def test_exceptions_table_is_respected() -> None:
    measured = checker.aggregate_packages(
        _report(
            {
                "src/forze_core/a.py": _file_entry(95, 100),
                "src/forze_legacy/a.py": _file_entry(65, 100),
            }
        )
    )
    floors = _floors(default=80.0, forze_legacy=60.0)
    ok = checker.check_floors(measured, floors, {"forze_core", "forze_legacy"})
    assert ok == []

    # The exception is a floor, not a free pass: dropping below it still fails.
    measured_worse = checker.aggregate_packages(
        _report(
            {
                "src/forze_core/a.py": _file_entry(95, 100),
                "src/forze_legacy/a.py": _file_entry(55, 100),
            }
        )
    )
    violations = checker.check_floors(
        measured_worse, floors, {"forze_core", "forze_legacy"}
    )
    assert len(violations) == 1
    assert "forze_legacy" in violations[0]
    assert "55.0%" in violations[0]
    assert "60.0%" in violations[0]


def test_unknown_new_package_gets_the_default_floor() -> None:
    # A brand-new package absent from the exceptions table must be gated at the
    # default floor automatically — that is the point of the finding.
    measured = checker.aggregate_packages(
        _report(
            {
                "src/forze_core/a.py": _file_entry(95, 100),
                "src/forze_brand_new/a.py": _file_entry(1, 100),
            }
        )
    )
    violations = checker.check_floors(
        measured, _floors(default=80.0), {"forze_core", "forze_brand_new"}
    )
    assert len(violations) == 1
    assert "forze_brand_new" in violations[0]
    assert "1.0%" in violations[0]
    assert "80.0%" in violations[0]
    assert "default" in violations[0]


def test_src_package_missing_from_coverage_data_fails() -> None:
    measured = checker.aggregate_packages(
        _report({"src/forze_core/a.py": _file_entry(95, 100)})
    )
    violations = checker.check_floors(
        measured, _floors(default=80.0), {"forze_core", "forze_unmeasured"}
    )
    assert len(violations) == 1
    assert "forze_unmeasured" in violations[0]
    assert "no coverage data" in violations[0]


def test_stale_exception_entry_fails() -> None:
    measured = checker.aggregate_packages(
        _report({"src/forze_core/a.py": _file_entry(95, 100)})
    )
    floors = _floors(default=80.0, forze_deleted=50.0)
    violations = checker.check_floors(measured, floors, {"forze_core"})
    assert len(violations) == 1
    assert "forze_deleted" in violations[0]
    assert "stale" in violations[0]


def test_all_above_floor_passes() -> None:
    measured = checker.aggregate_packages(
        _report(
            {
                "src/forze_a/a.py": _file_entry(80, 100),
                "src/forze_b/b.py": _file_entry(100, 100),
            }
        )
    )
    assert checker.check_floors(measured, _floors(), {"forze_a", "forze_b"}) == []


# ----------------------- #
# End-to-end via main()


def _write_fixture(
    tmp_path: Path,
    files: dict[str, dict[str, Any]],
    packages: list[str],
    pyproject_toml: str,
) -> list[str]:
    coverage_json = tmp_path / "coverage.json"
    coverage_json.write_text(json.dumps(_report(files)), encoding="utf-8")
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text(pyproject_toml, encoding="utf-8")
    src_root = tmp_path / "src"
    for package in packages:
        (src_root / package).mkdir(parents=True)
    return [
        str(coverage_json),
        "--pyproject",
        str(pyproject),
        "--src-root",
        str(src_root),
    ]


def test_main_passes_and_prints_table(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    argv = _write_fixture(
        tmp_path,
        files={"src/forze_a/a.py": _file_entry(90, 100)},
        packages=["forze_a"],
        pyproject_toml="[tool.coverage_floors]\ndefault = 80\n",
    )
    assert checker.main(argv) == 0
    out = capsys.readouterr().out
    assert "forze_a" in out
    assert "passed" in out


def test_main_fails_on_below_floor_package(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    argv = _write_fixture(
        tmp_path,
        files={
            "src/forze_a/a.py": _file_entry(90, 100),
            "src/forze_thin/a.py": _file_entry(5, 100),
        },
        packages=["forze_a", "forze_thin"],
        pyproject_toml="[tool.coverage_floors]\ndefault = 80\n",
    )
    assert checker.main(argv) == 1
    out = capsys.readouterr().out
    assert "forze_thin" in out
    assert "5.0%" in out
    assert "80.0%" in out


def test_main_fails_without_floors_table(tmp_path: Path) -> None:
    argv = _write_fixture(
        tmp_path,
        files={"src/forze_a/a.py": _file_entry(90, 100)},
        packages=["forze_a"],
        pyproject_toml="[tool.nothing]\n",
    )
    with pytest.raises(SystemExit):
        checker.main(argv)


# ----------------------- #
# Real repo policy shape


def test_repo_policy_is_well_formed() -> None:
    with (_REPO / "pyproject.toml").open("rb") as fh:
        config = tomllib.load(fh)
    table = config["tool"]["coverage_floors"]
    assert isinstance(table["default"], (int, float))
    assert table["default"] >= 75

    src_packages = checker.discover_src_packages(_REPO / "src")
    assert src_packages, "no forze* packages found under src/"
    exceptions = table.get("exceptions", {})
    for package, floor in exceptions.items():
        # Entries must name real packages and sit strictly below the default —
        # an entry at/above the default is dead weight that hides ratchet drift.
        assert package in src_packages, f"stale exceptions entry: {package}"
        assert floor < table["default"], f"pointless exceptions entry: {package}"
