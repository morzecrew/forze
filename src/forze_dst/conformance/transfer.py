"""The corpus transfer seam — do the mock's bug verdicts hold on the real engine?

The anomaly battery conforms the mock per *phenomenon*; this seam conforms it per **found bug**:
each transferable corpus mutant carries a :class:`TransferScript` — a hand-authored provocation
(a forced port-level interleaving or a plain re-invocation) plus a final-state verdict predicate
read back through the ports — runnable against any
:class:`~forze_dst.conformance.harness.ConformanceBackend`. :func:`run_transfer` runs every
script on the mock and on a real backend and records the verdict pairs; a divergence is classified
by direction, because the directions have opposite costs:

* :attr:`TransferClassification.MOCK_STRICT` — the mock detects, the real engine stays clean:
  the mutant is a **mock artifact**, and everything DST "catches" via it is noise.
* :attr:`TransferClassification.MOCK_WEAK` — the real engine bleeds, the mock stays clean:
  the dangerous direction — DST would green-light a real bug.

There is no allowed-divergence catalog here (unlike the anomaly matrix): a corpus mutant's whole
point is ground truth, so **any** divergence is a finding — a mock fidelity bug to fix or a
mislabeled mutant to reclassify, never data to park.
"""

from __future__ import annotations

import json
from collections.abc import Awaitable, Callable, Mapping, Sequence
from enum import Enum
from pathlib import Path
from typing import final

import attrs

from ..misuse import MisuseControl, MisuseMutant, TransferTier
from .harness import ConformanceBackend

# ----------------------- #


class Detection(Enum):
    """Whether the seeded defect manifested on a backend (the final-state predicate's verdict)."""

    DETECTED = "detected"
    CLEAN = "clean"


# ....................... #


class TransferClassification(Enum):
    """How a mutant's mock verdict relates to its real-backend verdict."""

    AGREE = "agree"
    MOCK_STRICT = "mock_strict"
    """Mock detects, real clean — the mutant is a mock artifact; its catches are noise."""
    MOCK_WEAK = "mock_weak"
    """Real bleeds, mock clean — DST would green-light a real bug."""


# ....................... #


@final
@attrs.frozen(kw_only=True)
class TransferScript:
    """One corpus instance's transfer form: provocation + port-read verdict, backend-agnostic.

    The script must be **self-isolating on a reused store** (fresh ids per invocation — the same
    discipline as the anomaly battery), because a real backend's tables persist across scripts.
    """

    mutant_id: str
    """The corpus ``mutant_id`` / ``control_id`` this script transfers."""

    expect_detected: bool
    """The corpus-side expectation: mutants ``True``, negative controls ``False``. The runner
    asserts the MOCK leg reproduces this — parity with the simulation verdict — before any
    mock↔real comparison means anything."""

    run: Callable[[ConformanceBackend], Awaitable[Detection]]


# ....................... #


@final
@attrs.frozen(kw_only=True)
class TransferRecord:
    """One (script, real backend) outcome: both verdicts and their classification."""

    mutant_id: str
    engine: str
    expect_detected: bool
    mock: Detection
    real: Detection
    classification: TransferClassification

    # ....................... #

    @property
    def mock_parity(self) -> bool:
        """Whether the mock leg reproduced the corpus verdict (mutant detected / control clean)."""

        return (self.mock is Detection.DETECTED) == self.expect_detected


# ....................... #


def _classify(mock: Detection, real: Detection) -> TransferClassification:
    if mock == real:
        return TransferClassification.AGREE

    if mock is Detection.DETECTED:
        return TransferClassification.MOCK_STRICT

    return TransferClassification.MOCK_WEAK


# ....................... #


async def run_transfer(
    scripts: Sequence[TransferScript],
    *,
    mock_backend: ConformanceBackend,
    real_backend: ConformanceBackend,
) -> tuple[TransferRecord, ...]:
    """Run every script on both backends; record the verdict pair per instance."""

    records: list[TransferRecord] = []

    for script in scripts:
        mock = await script.run(mock_backend)
        real = await script.run(real_backend)
        records.append(
            TransferRecord(
                mutant_id=script.mutant_id,
                engine=real_backend.scope_name,
                expect_detected=script.expect_detected,
                mock=mock,
                real=real,
                classification=_classify(mock, real),
            )
        )

    return tuple(records)


# ....................... #


def divergences(records: Sequence[TransferRecord]) -> tuple[TransferRecord, ...]:
    """Every finding: a mock↔real disagreement in either direction, or a lost mock parity.

    Parity loss is a finding even when the backends agree — a mutant both backends cleared is
    an ``AGREE`` record that evidences nothing (the corpus expectation was never reproduced),
    and must never fold into a green differential.
    """

    return tuple(
        record
        for record in records
        if record.classification is not TransferClassification.AGREE or not record.mock_parity
    )


# ....................... #


def write_transfer(records: Sequence[TransferRecord], directory: str | Path) -> Path:
    """Write the records to ``transfer_<engine>.json`` under *directory*; return the path."""

    if not records:
        raise ValueError("no transfer records to write")

    engine = records[0].engine
    target = Path(directory) / f"transfer_{engine}.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        json.dumps(
            [
                {
                    "mutant_id": record.mutant_id,
                    "engine": record.engine,
                    "expect_detected": record.expect_detected,
                    "mock": record.mock.value,
                    "real": record.real.value,
                    "classification": record.classification.value,
                }
                for record in records
            ],
            indent=2,
        )
        + "\n"
    )
    return target


# ....................... #

_GLYPHS = {"agree": "✓", "mock_strict": "▲", "mock_weak": "△"}


def _verdict_text(record: Mapping[str, object]) -> str:
    classification = str(record["classification"])
    detected = record["mock"] == Detection.DETECTED.value
    parity = detected == bool(record["expect_detected"])

    text = f"{_GLYPHS[classification]} {classification.replace('_', '-')}"
    return text if parity else f"{text} · **parity lost**"


# ....................... #


def render_transfer_markdown(
    records: Sequence[Mapping[str, object]],
    *,
    corpus: Sequence[MisuseMutant],
    controls: Sequence[MisuseControl],
) -> str:
    """Render transfer records (as written by :func:`write_transfer`) against their registry.

    The registry join is strict in both directions — a record without a corpus/control entry, or
    a transferable instance without a record on some engine, is a rendering error, not a silent
    gap. The ``NOT_TRANSFERABLE`` fraction is reported with each mutant's stated reason, so a
    capped denominator can never read as full coverage.
    """

    mutants = {mutant.mutant_id: mutant for mutant in corpus}
    control_ids = {control.control_id for control in controls}
    transferable = [
        mutant for mutant in corpus if mutant.transfer_tier is not TransferTier.NOT_TRANSFERABLE
    ]
    untransferable = [
        mutant for mutant in corpus if mutant.transfer_tier is TransferTier.NOT_TRANSFERABLE
    ]

    by_engine: dict[str, dict[str, Mapping[str, object]]] = {}
    for record in records:
        instance = str(record["mutant_id"])
        if instance not in mutants and instance not in control_ids:
            raise ValueError(f"transfer record {instance!r} has no corpus/control entry")
        by_engine.setdefault(str(record["engine"]), {})[instance] = record

    lines: list[str] = [
        "# Corpus bug transfer",
        "",
        "Mock↔real verdict pairs for every transferable misuse-corpus instance: the same",
        "hand-authored provocation (a forced port-level interleaving, a re-invocation, or a",
        "documented crash analog) runs on both backends, and the same final-state predicate —",
        "read back through the ports — decides `detected`/`clean` on each. `✓` = the verdicts",
        "agree; `▲` = **mock artifact** (mock detects, real clean — everything DST catches via",
        "that mutant is noise); `△` = **mock blind spot** (real bleeds, mock clean — the",
        "dangerous direction: DST would green-light a real bug). There is no allowed-divergence",
        "catalog on this plane — any divergence is a finding.",
        "",
        "*Generated by `just dst-transfer` — do not edit by hand.*",
        "",
    ]

    for engine, rows in by_engine.items():
        missing = [
            instance
            for instance in [m.mutant_id for m in transferable] + sorted(control_ids)
            if instance not in rows
        ]
        if missing:
            raise ValueError(f"engine {engine!r} is missing transfer records for {missing}")

        strict = sum(1 for r in rows.values() if r["classification"] == "mock_strict")
        weak = sum(1 for r in rows.values() if r["classification"] == "mock_weak")

        lines += [
            f"## mock ↔ {engine}",
            "",
            (
                f"Instances: **{len(transferable)}** mutants + **{len(control_ids)}** controls · "
                f"mock artifacts (▲): **{strict}** · mock blind spots (△): **{weak}**"
            ),
            "",
            "| mutant | family | tier | mock | real | verdict |",
            "|---|---|---|---|---|---|",
        ]
        for mutant in transferable:
            record = rows[mutant.mutant_id]
            lines.append(
                f"| `{mutant.mutant_id}` | {mutant.family.value} "
                f"| {mutant.transfer_tier.value.replace('_', '-')} "
                f"| {record['mock']} | {record['real']} | {_verdict_text(record)} |"
            )

        lines += [
            "",
            "### Controls (expected clean on both backends)",
            "",
            "| control | mock | real | verdict |",
            "|---|---|---|---|",
        ]
        for control in controls:
            record = rows[control.control_id]
            lines.append(
                f"| `{control.control_id}` | {record['mock']} | {record['real']} "
                f"| {_verdict_text(record)} |"
            )

        lines.append("")

    lines += [
        f"### Not transferable — {len(untransferable)}/{len(corpus)} mutants",
        "",
        "Defects whose trigger or observable needs simulation-only machinery; their",
        "`ground_truth` stays undetermined by design, and the fraction is stated so a capped",
        "denominator never reads as full coverage.",
        "",
        "| mutant | reason |",
        "|---|---|",
    ]
    for mutant in untransferable:
        lines.append(f"| `{mutant.mutant_id}` | {mutant.notes} |")

    lines.append("")
    return "\n".join(lines)
