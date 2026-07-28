"""The fidelity agreement matrix — the differential's verdicts as a durable, direction-split artifact.

The mock↔real differential runs assert per-cell and discard what they observed; this module makes
the measurement durable. :func:`collect_verdicts` runs the battery against any backend and records
one :class:`CellVerdict` per ``(case, level)``; :meth:`FidelityMatrix.pair` joins a mock collection
with a real backend's collection into per-cell classifications, split — always — by divergence
*direction*, because the two directions have opposite costs:

* :attr:`Classification.MOCK_STRICT` — the mock prevents an anomaly the real engine permits.
  Simulation green, production bleeds: these are the bugs you would ship.
* :attr:`Classification.MOCK_WEAK` — the mock permits an anomaly the real engine prevents.
  Simulation reports violations reality cannot produce: wasted time and eroded trust.

There is deliberately **no** collapsed agreement score — a single number would average two failure
modes with opposite costs into meaninglessness. A divergence is *explained* only by a reviewed
engine-scoped :data:`~forze_dst.conformance.divergence.CONTRACT_STRENGTHENINGS` entry (e.g. Mongo's
read-committed-is-snapshot); anything else lands in :attr:`FidelityMatrix.unexplained`, which the
differential gate asserts empty — the artifact never becomes a place to park a real disagreement.
:data:`~forze_dst.conformance.divergence.MECHANISM_DIVERGENCES` never appear as explanations: they
are normalized away *before* a verdict exists, and are rendered in the report for completeness only.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from enum import Enum
from pathlib import Path
from typing import cast, final

import attrs

from forze.application.contracts.transaction import IsolationLevel

from .anomalies import BATTERY, AnomalyCase
from .divergence import CONTRACT_STRENGTHENINGS, MECHANISM_DIVERGENCES
from .harness import ConformanceBackend, Verdict

# ----------------------- #


class Classification(Enum):
    """How a paired ``(mock, real)`` cell relates — agreement, or one of the two divergence directions."""

    AGREE = "agree"
    MOCK_STRICT = "mock_strict"
    """Mock prevents, real permits — the false-confidence direction (simulation green, production bleeds)."""
    MOCK_WEAK = "mock_weak"
    """Mock permits, real prevents — the false-alarm direction (violations reality cannot produce)."""


# ....................... #


@final
@attrs.frozen(kw_only=True)
class CellVerdict:
    """One observed verdict: a battery case run against one backend at one level."""

    case: str
    adya: str
    level: IsolationLevel
    verdict: Verdict


# ....................... #


async def collect_verdicts(
    backend: ConformanceBackend,
    *,
    levels: Sequence[IsolationLevel],
    cases: Sequence[AnomalyCase] = BATTERY,
) -> tuple[CellVerdict, ...]:
    """Run *cases* × *levels* against *backend*, recording the observed verdict per cell."""

    cells: list[CellVerdict] = []

    for case in cases:
        for level in levels:
            observed = await case.run(backend, level)
            cells.append(CellVerdict(case=case.name, adya=case.adya, level=level, verdict=observed))

    return tuple(cells)


# ....................... #


@final
@attrs.frozen(kw_only=True)
class MatrixCell:
    """One paired cell of the matrix: both observed verdicts and their classification."""

    case: str
    adya: str
    level: IsolationLevel
    mock: Verdict
    real: Verdict
    classification: Classification
    explained_by: str | None
    """The reviewed catalog entry justifying a divergence (``strengthening:<case>@<LEVEL>[<engine>]``),
    or ``None`` — meaning either agreement (nothing to explain) or an UNEXPLAINED divergence."""

    # ....................... #

    @property
    def unexplained(self) -> bool:
        """A divergence no reviewed catalog entry covers — a real disagreement, never acceptable."""

        return self.classification is not Classification.AGREE and self.explained_by is None


# ....................... #


def _explain(case: AnomalyCase, level: IsolationLevel, *, engine: str, real: Verdict) -> str | None:
    """The catalog tag justifying a mock↔real divergence at this cell, or ``None``.

    A divergence is explained only when an **engine-scoped** strengthening entry for this exact
    ``(case, level, engine)`` predicts the real side's verdict — i.e. the real engine deviates from
    the mock precisely because the reviewed catalog says this engine is stronger here.
    """

    for entry in CONTRACT_STRENGTHENINGS:
        if (
            entry.engine == engine
            and entry.anomaly == case.name
            and entry.level == level
            and entry.observed == real
        ):
            return f"strengthening:{case.name}@{level.name}[{engine}]"

    return None


# ....................... #


@final
@attrs.frozen(kw_only=True)
class FidelityMatrix:
    """The mock↔real agreement matrix for one real backend, direction-split, gateable."""

    engine: str
    """The real backend's scope name (``"postgres"``, ``"mongo"``, …)."""

    levels: tuple[IsolationLevel, ...]
    """The levels the real leg ran (an engine without a level contributes no cells for it)."""

    cells: tuple[MatrixCell, ...]

    # ....................... #

    @classmethod
    def pair(
        cls,
        mock_cells: Sequence[CellVerdict],
        real_cells: Sequence[CellVerdict],
        *,
        engine: str,
    ) -> FidelityMatrix:
        """Join a mock collection with a real backend's collection into a classified matrix.

        Cells pair by ``(case, level)``; only pairs present on **both** sides become matrix cells
        (the real leg's level set is the binding one — e.g. Mongo contributes no SERIALIZABLE row).
        A missing mock counterpart for an observed real cell is a collection bug and raises.
        """

        by_name = {case.name: case for case in BATTERY}
        mock_by_key = {(cell.case, cell.level): cell for cell in mock_cells}

        cells: list[MatrixCell] = []
        levels: list[IsolationLevel] = []

        for real in real_cells:
            mock = mock_by_key.get((real.case, real.level))
            if mock is None:
                raise ValueError(f"no mock verdict collected for {real.case}@{real.level.name}")

            if real.level not in levels:
                levels.append(real.level)

            if mock.verdict == real.verdict:
                classification = Classification.AGREE
                explained = None
            else:
                classification = (
                    Classification.MOCK_STRICT
                    if mock.verdict is Verdict.PREVENTED
                    else Classification.MOCK_WEAK
                )
                explained = _explain(
                    by_name[real.case], real.level, engine=engine, real=real.verdict
                )

            cells.append(
                MatrixCell(
                    case=real.case,
                    adya=real.adya,
                    level=real.level,
                    mock=mock.verdict,
                    real=real.verdict,
                    classification=classification,
                    explained_by=explained,
                )
            )

        return cls(engine=engine, levels=tuple(levels), cells=tuple(cells))

    # ....................... #

    @property
    def mock_strict(self) -> tuple[MatrixCell, ...]:
        """The false-confidence direction: mock prevents, real permits — bugs you would ship."""

        return tuple(
            cell for cell in self.cells if cell.classification is Classification.MOCK_STRICT
        )

    # ....................... #

    @property
    def mock_weak(self) -> tuple[MatrixCell, ...]:
        """The false-alarm direction: mock permits, real prevents — violations reality can't produce."""

        return tuple(cell for cell in self.cells if cell.classification is Classification.MOCK_WEAK)

    # ....................... #

    @property
    def unexplained(self) -> tuple[MatrixCell, ...]:
        """Divergences no reviewed catalog entry covers — the gate asserts this empty."""

        return tuple(cell for cell in self.cells if cell.unexplained)

    # ....................... #

    def to_payload(self) -> dict[str, object]:
        """A JSON-ready snapshot of the matrix (stable ordering, enum values as strings)."""

        return {
            "engine": self.engine,
            "levels": [level.name for level in self.levels],
            "cells": [
                {
                    "case": cell.case,
                    "adya": cell.adya,
                    "level": cell.level.name,
                    "mock": cell.mock.value,
                    "real": cell.real.value,
                    "classification": cell.classification.value,
                    "explained_by": cell.explained_by,
                }
                for cell in self.cells
            ],
        }


# ....................... #


def write_matrix(matrix: FidelityMatrix, directory: str | Path) -> Path:
    """Write the matrix payload to ``fidelity_<engine>.json`` under *directory*; return the path."""

    target = Path(directory) / f"fidelity_{matrix.engine}.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(matrix.to_payload(), indent=2) + "\n")
    return target


# ....................... #

_GLYPHS = {
    "agree": "✓",
    "mock_strict": "▲",  # mock stricter — false-confidence direction
    "mock_weak": "△",  # mock weaker — false-alarm direction
}


def _cell_text(cell: Mapping[str, object]) -> str:
    classification = str(cell["classification"])
    glyph = _GLYPHS[classification]

    if classification == "agree":
        return f"{glyph} {cell['mock']}"

    suffix = "explained" if cell["explained_by"] else "**UNEXPLAINED**"
    return f"{glyph} mock {cell['mock']} / real {cell['real']} ({suffix})"


# ....................... #


def render_markdown(payloads: Sequence[Mapping[str, object]]) -> str:
    """Render matrix payloads (one per real backend) plus both catalogs as one markdown document.

    The tables always separate the two divergence directions — never a single agreement score —
    and re-render the catalogs from the live module, so the document can't drift from the reviewed
    data it presents.
    """

    lines: list[str] = [
        "# DST fidelity matrix",
        "",
        "Mock↔real verdict agreement for the isolation anomaly battery, per backend and",
        "`IsolationLevel`. `✓` = both backends produce the same verdict; `▲` = **mock stricter**",
        "(mock prevents, real permits — the false-confidence direction: simulation green,",
        "production bleeds); `△` = **mock weaker** (mock permits, real prevents — the false-alarm",
        "direction: simulated violations reality cannot produce). A divergence is admissible only",
        "when a reviewed engine-scoped catalog entry explains it; an unexplained divergence fails",
        "the differential. There is deliberately no single agreement score.",
        "",
        "*Generated by `just dst-fidelity` — do not edit by hand.*",
        "",
    ]

    for payload in payloads:
        engine = payload["engine"]
        levels = [str(level) for level in cast("Sequence[object]", payload["levels"])]
        cells = cast("Sequence[Mapping[str, object]]", payload["cells"])

        by_case: dict[tuple[str, str], dict[str, Mapping[str, object]]] = {}
        for cell in cells:
            key = (str(cell["case"]), str(cell["adya"]))
            by_case.setdefault(key, {})[str(cell["level"])] = cell

        strict = sum(1 for c in cells if c["classification"] == "mock_strict")
        weak = sum(1 for c in cells if c["classification"] == "mock_weak")
        unexplained = sum(
            1 for c in cells if c["classification"] != "agree" and not c["explained_by"]
        )

        lines += [
            f"## mock ↔ {engine}",
            "",
            (
                f"Divergences — mock-stricter (▲): **{strict}** · mock-weaker (△): **{weak}** · "
                f"unexplained: **{unexplained}**"
            ),
            "",
            "| case | Adya | " + " | ".join(levels) + " |",
            "|---|---|" + "---|" * len(levels),
        ]

        for (case, adya), row in by_case.items():
            rendered = [_cell_text(row[level]) if level in row else "—" for level in levels]
            lines.append(f"| `{case}` | {adya} | " + " | ".join(rendered) + " |")

        lines.append("")

    lines += [
        "## Contract strengthenings (reviewed)",
        "",
        "A Forze adapter prevents an anomaly the textbook contract permits — the only sanctioned",
        "way an observed verdict may deviate from the textbook, and the only thing that may",
        "explain a mock↔real divergence above.",
        "",
        "| anomaly | level | engine | contract → observed | source |",
        "|---|---|---|---|---|",
    ]

    for entry in CONTRACT_STRENGTHENINGS:
        lines.append(
            f"| `{entry.anomaly}` | {entry.level.name} | {entry.engine or '*'} "
            f"| {entry.contract.value} → {entry.observed.value} | {entry.source} |"
        )

    lines += [
        "",
        "## Mechanism divergences (normalized, never verdict-level)",
        "",
        "Surface differences between correct engines that the differential normalizes away before",
        "a verdict exists — listed for completeness; they can never explain a verdict divergence.",
        "",
        "| name | source |",
        "|---|---|",
    ]

    for divergence in MECHANISM_DIVERGENCES:
        lines.append(f"| `{divergence.name}` | {divergence.source} |")

    lines.append("")
    return "\n".join(lines)
