"""The W3 bound-comparison scan (.github/scripts/analyze_campaign.py) — its multiplicity guards.

The scan checks every (mutant, strategy) cell against the PCT floor and reports one
``Bound violations`` count. Two things about that arrangement are load-bearing, and both are
easy to get quietly wrong:

- **Family-wise error.** Per-cell 95% across ~15 cells makes a spurious flag likelier than not,
  and the analysis says exactly what a violation would mean (*"a wrong depth label or wrong n/k
  accounting"*), so a false alarm costs a reviewer a re-derivation of something correct. The
  per-cell level must be Šidák-corrected to the number of cells the scan **actually** covered —
  not to the number of groups in the file, and not to a constant.
- **The flip margin.** ``p̂_sched = p̂ / p_trigger`` carries an interval on ``p̂`` and none at all on
  ``p_trigger``. Each cell states the exact factor by which ``p_trigger`` would have to be wrong
  to flip its verdict, and a flip needing ``p_trigger > 1`` must read as *unreachable* rather than
  as a number implying an impossible probability.

Driven with synthetic records so the cell count, the trigger, and the verdict are all controlled.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

from forze_dst.stats import sidak_level

# ----------------------- #

_REPO = Path(__file__).resolve().parents[2]
_SCRIPT = _REPO / ".github" / "scripts" / "analyze_campaign.py"


def _load_analysis() -> ModuleType:
    spec = importlib.util.spec_from_file_location("analyze_campaign", _SCRIPT)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load analysis script at {_SCRIPT}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


analysis = _load_analysis()


def _record(mutant_id: str, strategy: str, *, detected_at: int | None, trials: int) -> str:
    return json.dumps(
        {
            "kind": "campaign",
            "mutant_id": mutant_id,
            "strategy": strategy,
            "detection_trial": detected_at,
            "trials_run": trials,
            "max_tasks": 2,
            "max_choice_steps": 4,
        }
    )


def _run(tmp_path: Path, records: list[str]) -> str:
    """Run the analysis over *records* and return the appended markdown."""

    tmp_path.mkdir(parents=True, exist_ok=True)
    raw = tmp_path / "campaign.jsonl"
    raw.write_text("\n".join(records))
    summary = tmp_path / "summary.md"
    summary.write_text("# head\n")

    assert analysis.main([str(raw), "--summary", str(summary)]) == 0

    return summary.read_text()


def _cells(text: str) -> list[str]:
    return [line for line in text.splitlines() if line.startswith("| `")]


def _column(row: str, name: str, header: str) -> str:
    """The value of column *name* in *row*, located by the table *header*."""

    names = [cell.strip() for cell in header.strip().strip("|").split("|")]
    values = [cell.strip() for cell in row.strip().strip("|").split("|")]

    return values[names.index(name)]


def _header(text: str) -> str:
    return next(line for line in text.splitlines() if line.startswith("| mutant |"))


# ....................... #


class TestFamilyWiseControl:
    """The correction divides by the cells the scan covered — measured, not assumed."""

    @staticmethod
    def _corpus_records(mutant_ids: list[str]) -> list[str]:
        return [
            _record(mutant_id, strategy, detected_at=trial, trials=20)
            for mutant_id in mutant_ids
            for strategy in ("pct-d2", "pct-d3")
            for trial in (3, 5, 8, None)
        ]

    def test_the_stated_level_matches_the_cell_count_actually_scanned(
        self, tmp_path: Path
    ) -> None:
        text = _run(tmp_path, self._corpus_records(["T1-blind-write-payment", "T2-charge-before-guard"]))
        scanned = len(_cells(text))

        assert scanned == 4  # 2 mutants × 2 PCT strategies, all depth-1 so both parameters apply
        assert f"Šidák over {scanned}" in text
        assert f"{sidak_level(0.95, scanned):.4%} per cell" in text
        assert "95% family-wise" in text

    def test_the_level_tightens_as_the_scan_widens(self, tmp_path: Path) -> None:
        narrow = _run(tmp_path / "a", self._corpus_records(["T1-blind-write-payment"]))
        wide = _run(
            tmp_path / "b",
            self._corpus_records(
                ["T1-blind-write-payment", "T2-charge-before-guard", "T5-unchecked-reservation"]
            ),
        )

        assert f"{sidak_level(0.95, 2):.4%} per cell" in narrow
        assert f"{sidak_level(0.95, 6):.4%} per cell" in wide
        assert sidak_level(0.95, 6) > sidak_level(0.95, 2)

    def test_excluded_mutants_do_not_inflate_the_denominator(self, tmp_path: Path) -> None:
        # A mutant whose trigger is a fault lottery is not scanned, so correcting for it would
        # widen every other cell's interval for a comparison that was never made.
        with_excluded = self._corpus_records(
            ["T1-blind-write-payment", "M1-dual-write-shipment"]
        )

        text = _run(tmp_path, with_excluded)

        assert len(_cells(text)) == 2
        assert f"Šidák over {2}" in text
        assert "`M1-dual-write-shipment`" in text  # named in the exclusions, never silent

    def test_the_violation_count_is_reported_with_its_family_level(self, tmp_path: Path) -> None:
        text = _run(tmp_path, self._corpus_records(["T1-blind-write-payment"]))

        assert "**Bound violations: 0** (family-wise 95% over 2 cells)." in text

    def test_the_injected_regression_a_constant_level_would_fail_this(
        self, tmp_path: Path
    ) -> None:
        # Correcting to a fixed m (or not correcting at all) is what this gate exists to catch:
        # the stated per-cell level would then not track the scan's own width.
        narrow = _run(tmp_path / "a", self._corpus_records(["T1-blind-write-payment"]))
        wide = _run(
            tmp_path / "b",
            self._corpus_records(["T1-blind-write-payment", "T2-charge-before-guard"]),
        )

        assert f"{sidak_level(0.95, 2):.4%} per cell" in narrow
        assert f"{sidak_level(0.95, 2):.4%} per cell" not in wide
        assert f"{0.95:.4%} per cell" not in narrow  # nor the uncorrected level


# ....................... #


class TestFlipMarginColumn:
    def test_every_scanned_cell_carries_a_margin(self, tmp_path: Path) -> None:
        records = [
            _record("T1-blind-write-payment", "pct-d2", detected_at=trial, trials=20)
            for trial in (3, 5, 8, None)
        ]

        text = _run(tmp_path, records)
        header = _header(text)

        assert "flip margin" in header
        for row in _cells(text):
            assert _column(row, "flip margin", header)

    def test_an_unreachable_flip_says_so_rather_than_quoting_a_factor(
        self, tmp_path: Path
    ) -> None:
        # A depth-1 mutant with p_trigger = 1.0 and a high detection rate: p̂_upper / bound > 1,
        # so no admissible p_trigger flips the verdict and a factor would imply p > 1.
        records = [
            _record("N2-stale-cache", "pct-d2", detected_at=1, trials=20) for _ in range(20)
        ]

        text = _run(tmp_path, records)
        header = _header(text)
        margins = [_column(row, "flip margin", header) for row in _cells(text)]

        assert margins == ["unreachable"]
        assert not any("×" in margin for margin in margins)

    def test_a_reachable_flip_is_quoted_as_a_factor(self, tmp_path: Path) -> None:
        # A collision-pool mutant (p_trigger = 1/16) against the depth-1 floor of 1/2: the
        # conditional upper limit sits just above the bound, so the margin is small and finite.
        records = [
            _record("T1-blind-write-payment", "pct-d2", detected_at=None, trials=20)
            for _ in range(15)
        ] + [_record("T1-blind-write-payment", "pct-d2", detected_at=9, trials=20)]

        text = _run(tmp_path, records)
        header = _header(text)
        margin = _column(_cells(text)[0], "flip margin", header)

        assert margin.endswith("×")
        assert float(margin.rstrip("×")) > 0.0

    def test_the_method_note_records_why_n_and_k_carry_no_interval(self, tmp_path: Path) -> None:
        # A later reader must not mistake the absence of an interval on the maxima for an
        # oversight and "fix" it — the bias is real but points the conservative way.
        text = _run(
            tmp_path,
            [_record("T1-blind-write-payment", "pct-d2", detected_at=3, trials=20)],
        )

        assert "carry no interval on purpose" in text
        assert "most conservative floor" in text


# ....................... #


@pytest.mark.parametrize(
    ("mutant_id", "expected"),
    [
        ("M1-dual-write-shipment", None),  # crash-stream lottery
        ("I3-ack-before-processing", None),
        ("N1-drop-tenant-predicate", None),  # uninstrumented workload-order lottery
        ("D4-unmerged-remote-hlc", None),
    ],
)
def test_mutants_the_theorem_does_not_speak_about_are_excluded_by_name(
    tmp_path: Path, mutant_id: str, expected: Any
) -> None:
    text = _run(
        tmp_path, [_record(mutant_id, "pct-d3", detected_at=3, trials=20)]
    )

    assert expected is None
    assert _cells(text) == []
    assert f"`{mutant_id}`" in text
    # No cell scanned means no family to correct: stating a level over zero comparisons would
    # name a correction the code did not apply.
    assert "Šidák over" not in text
    assert "**Multiplicity.**" not in text
