"""The oracle — assert over a recorded history, then shrink a violation to a counterexample.

Submodules: ``recorder`` (the :class:`History` substrate), ``invariants`` + ``linearizability`` +
``reachability`` (the assertions — what must always / sometimes hold), ``report`` (the causal-graph
renderer), ``coverage`` (the behavioural signal + fingerprint), and ``replay`` (explore → minimize →
:class:`ViolationReport`). This package ``__init__`` re-exports the replay API so
``from forze_dst.oracle import ViolationReport`` keeps working; the submodules are imported by path.
"""

from __future__ import annotations

from forze_dst.oracle.replay import (
    ViolationReport,
    explore,
    minimize,
    run_recorded,
)

__all__ = [
    "ViolationReport",
    "explore",
    "minimize",
    "run_recorded",
]
