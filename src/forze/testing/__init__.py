"""Test utilities for Forze apps — the support a handler test reaches for, shipped in core.

Three things today:

* **Build a context** — :func:`context_from_modules` / :func:`context_from_deps` wire an
  :class:`~forze.application.execution.ExecutionContext` to in-memory adapters, so a unit test can
  call a handler against a ``MockDepsModule`` with no runtime or transport.
* **Gate a whole surface** — :func:`assert_pure_module`, :func:`assert_scope_first` and
  :func:`assert_operation_namespaces` check a *property* over everything discovery finds —
  imports, Protocol signatures, operation ids — and refuse an empty discovery, so the gate
  cannot pass vacuously after a rename.
* **Force an interleaving** — :class:`Conductor` + :class:`Gate` drive concurrent coroutines through
  an exact, reproducible schedule, so a concurrency or isolation test is deterministic instead of
  flaky (the substrate for adapter conformance).

For exhaustive, seed-driven exploration of those same concerns, reach past this to Deterministic
Simulation Testing (:mod:`forze_dst`).
"""

from forze.testing.context import (
    context_from_deps,
    context_from_modules,
    frozen_deps_from_deps,
    frozen_deps_from_modules,
)
from forze.testing.gates import (
    assert_operation_namespaces,
    assert_pure_module,
    assert_scope_first,
)
from forze.testing.interleaving import Conductor, Gate, Session

__all__ = [
    "Conductor",
    "Gate",
    "Session",
    "assert_operation_namespaces",
    "assert_pure_module",
    "assert_scope_first",
    "context_from_deps",
    "context_from_modules",
    "frozen_deps_from_deps",
    "frozen_deps_from_modules",
]
