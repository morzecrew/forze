"""Test utilities for Forze apps — the support a handler test reaches for, shipped in core.

Two things today:

* **Build a context** — :func:`context_from_modules` / :func:`context_from_deps` wire an
  :class:`~forze.application.execution.ExecutionContext` to in-memory adapters, so a unit test can
  call a handler against a ``MockDepsModule`` with no runtime or transport.
* **Force an interleaving** — :class:`Conductor` + :class:`Gate` drive concurrent coroutines through
  an exact, reproducible schedule, so a concurrency or isolation test is deterministic instead of
  flaky (the substrate for adapter conformance).

For exhaustive, seed-driven exploration of those same concerns, reach past this to Deterministic
Simulation Testing (:mod:`forze_dst`).
"""

from __future__ import annotations

from forze.testing.context import (
    context_from_deps,
    context_from_modules,
    frozen_deps_from_deps,
    frozen_deps_from_modules,
)
from forze.testing.interleaving import Conductor, Gate, Session

__all__ = [
    "context_from_modules",
    "context_from_deps",
    "frozen_deps_from_modules",
    "frozen_deps_from_deps",
    "Conductor",
    "Gate",
    "Session",
]
