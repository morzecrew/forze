"""A regression seed corpus — turn a found counterexample into a permanent test.

The last link in the DST product loop: *find → reproduce → minimize → **regress***. When a
sweep finds a violating seed, append it (with the context needed to trust a replay — the
operation-catalog fingerprint, the violated invariants, the import target) to a corpus file;
a later ``replay`` re-runs exactly those seeds so the bug stays caught forever. The file is
JSON Lines (one self-describing entry per line) so it appends cheaply, reads back exactly, and
merges without conflict.

A changed registry (different ``registry_fingerprint``) means the code under test moved on and
a stored seed can no longer be trusted to reproduce — :func:`load_regressions` surfaces the
fingerprint so the caller can warn rather than silently pass.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING, Any, final

import attrs

from forze_dst.oracle.coverage import behavioral_fingerprint

if TYPE_CHECKING:
    from forze_dst.oracle import ViolationReport
    from forze_dst.oracle.recorder import History

# ----------------------- #


@final
@attrs.define(frozen=True, kw_only=True)
class RegressionEntry:
    """One saved counterexample: the seed (+ schedule seed) and the context to trust a replay."""

    seed: int
    schedule_seed: int | None = None
    target: str | None = None
    """The ``module:attr`` import string the seed was found against (for ``replay``)."""
    registry_fingerprint: str | None = None
    """The operation-catalog fingerprint at find time; a replay against a different fingerprint
    cannot be trusted to reproduce (the code under test changed)."""
    invariants: tuple[str, ...] = ()
    """Names of the invariants that were violated."""
    found_at: str | None = None
    """When the seed was saved (ISO-8601), stamped by the caller (real wall time)."""
    explore: dict[str, Any] | None = None
    """Snapshot of the exploration knobs the seed was found under (strategy / scheduler /
    act_count / faults / …). ``replay`` reproduces with these rather than the current CLI flags,
    so a regression found under one configuration is not silently reported clean under another."""

    behavioral_fingerprint: str | None = None
    """Opt-in handler-logic signature (:func:`~forze_dst.oracle.coverage.behavioral_fingerprint`) of the
    run that found the seed — an ordered, PII-free digest of its execution-trace shape. ``None``
    (default) keeps the structural-only posture. When set, a replay whose behavior digest differs
    has *drifted* (the code's logic moved on even if its contracts didn't); see
    :meth:`behavior_drifted`."""

    # ....................... #

    def to_json(self) -> str:
        """Render as a single JSON-Lines record."""

        return json.dumps(
            {
                "seed": self.seed,
                "schedule_seed": self.schedule_seed,
                "target": self.target,
                "registry_fingerprint": self.registry_fingerprint,
                "invariants": list(self.invariants),
                "found_at": self.found_at,
                "explore": self.explore,
                "behavioral_fingerprint": self.behavioral_fingerprint,
            }
        )

    # ....................... #

    @classmethod
    def from_json(cls, line: str) -> RegressionEntry:
        """Parse one JSON-Lines record (tolerant of missing optional keys)."""

        data = json.loads(line)

        return cls(
            seed=int(data["seed"]),
            schedule_seed=data.get("schedule_seed"),
            target=data.get("target"),
            registry_fingerprint=data.get("registry_fingerprint"),
            invariants=tuple(data.get("invariants", ())),
            found_at=data.get("found_at"),
            explore=data.get("explore"),
            behavioral_fingerprint=data.get("behavioral_fingerprint"),
        )

    # ....................... #

    def behavior_drifted(self, history: "History") -> bool:
        """Whether a replay's *history* diverges from the stored handler-logic signature.

        ``True`` only when a :attr:`behavioral_fingerprint` was recorded *and* the replay's
        digest differs — the strict, opt-in drift check. With no stored fingerprint (the default
        structural posture) it is always ``False``: drift can't be told, so it isn't claimed.
        """

        if self.behavioral_fingerprint is None:
            return False

        return behavioral_fingerprint(history) != self.behavioral_fingerprint


# ....................... #


def entry_from_report(
    report: ViolationReport,
    *,
    target: str | None = None,
    found_at: str | None = None,
    explore: dict[str, Any] | None = None,
    strict_behavior: bool = False,
) -> RegressionEntry:
    """Build a :class:`RegressionEntry` from a violating report (+ the caller's context).

    *explore* is the exploration-knob snapshot needed to reproduce (so ``replay`` does not
    depend on the current CLI flags matching the find). With *strict_behavior* the entry also
    records the run's :func:`~forze_dst.oracle.coverage.behavioral_fingerprint`, so a later replay can
    warn when the handler logic has drifted (see :meth:`RegressionEntry.behavior_drifted`).
    """

    return RegressionEntry(
        seed=report.seed,
        schedule_seed=report.schedule_seed,
        target=target,
        registry_fingerprint=report.registry_fingerprint,
        invariants=tuple(sorted({v.invariant for v in report.violations})),
        found_at=found_at,
        explore=explore,
        behavioral_fingerprint=(
            behavioral_fingerprint(report.history) if strict_behavior else None
        ),
    )


# ....................... #


def append_regression(path: str | Path, entry: RegressionEntry) -> None:
    """Append *entry* to the corpus at *path* (creating parent dirs / the file as needed).

    Idempotent on ``(seed, schedule_seed, target)``: a seed already in the corpus is not
    duplicated, so re-running the same find twice keeps the corpus tidy.
    """

    file = Path(path)

    for existing in load_regressions(file):
        if (existing.seed, existing.schedule_seed, existing.target) == (
            entry.seed,
            entry.schedule_seed,
            entry.target,
        ):
            return

    file.parent.mkdir(parents=True, exist_ok=True)

    with file.open("a", encoding="utf-8") as handle:
        handle.write(entry.to_json() + "\n")


# ....................... #


def load_regressions(path: str | Path) -> list[RegressionEntry]:
    """Load every corpus entry at *path*; an absent file is an empty corpus (no error).

    Blank lines are skipped so a hand-edited corpus stays readable.
    """

    file = Path(path)

    if not file.exists():
        return []

    return [
        RegressionEntry.from_json(line)
        for line in file.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
