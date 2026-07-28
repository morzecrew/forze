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
from collections.abc import Awaitable, Callable, Sequence
from enum import Enum
from pathlib import Path
from typing import final

import attrs

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
    """Every record whose verdicts disagree — all of them findings, in either direction."""

    return tuple(
        record for record in records if record.classification is not TransferClassification.AGREE
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
