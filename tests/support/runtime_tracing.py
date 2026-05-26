"""Helpers for runtime tracing in tests.

Import via ``from tests.support.runtime_tracing import ...`` when the test
package root is on ``PYTHONPATH`` (e.g. ``pytest`` with repo root as cwd).
Prefer ``traced_deps`` / ``traced_ctx`` fixtures in
``tests/unit/test_forze/application/conftest.py`` for unit tests.
"""

from __future__ import annotations

import attrs

from forze.application.execution import Deps, ExecutionContext
from forze.application.execution.tracing import (
    RuntimeTraceValidator,
    assert_runtime_trace_valid,
)
from forze_mock import MockDepsModule, MockState

# ----------------------- #


def build_traced_deps(
    mock_state: MockState,
    *,
    extra_plain: dict | None = None,
) -> Deps:
    """Build mock deps with ``trace_runtime=True``."""

    base = attrs.evolve(MockDepsModule(state=mock_state)(), trace_runtime=True)

    if not extra_plain:
        return base

    overlay = Deps.plain(extra_plain, trace_runtime=True)
    return Deps.merge(base, overlay)


def build_traced_ctx(deps: Deps) -> ExecutionContext:
    """Create an :class:`ExecutionContext` for *deps* with tracing session bound."""

    return ExecutionContext(deps=deps)


def assert_deps_runtime_trace_valid(
    deps: Deps,
    *validators: RuntimeTraceValidator,
) -> None:
    """Assert ``deps.runtime_trace()`` passes all *validators*."""

    assert_runtime_trace_valid(deps.runtime_trace(), *validators)
