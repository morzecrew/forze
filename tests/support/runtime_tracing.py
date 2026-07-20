"""Helpers for runtime tracing in tests."""

from __future__ import annotations

from forze.application.execution import Deps, DepsRegistry, ExecutionContext, FrozenDeps
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
) -> FrozenDeps:
    """Build mock deps with runtime tracing enabled via :class:`DepsRegistry`."""

    registry = DepsRegistry.from_modules(
        lambda: MockDepsModule(state=mock_state)(),
    )

    if extra_plain:
        registry = registry.with_deps(Deps.plain(extra_plain))

    return registry.with_tracing(runtime=True).freeze().resolve()


def build_traced_ctx(deps: FrozenDeps) -> ExecutionContext:
    """Create an :class:`ExecutionContext` for *deps* with tracing session bound."""

    return ExecutionContext(deps=deps)


def assert_deps_runtime_trace_valid(
    deps: FrozenDeps,
    *validators: RuntimeTraceValidator,
) -> None:
    """Assert ``deps.runtime_trace()`` passes all *validators*."""

    assert_runtime_trace_valid(deps.runtime_trace(), *validators)
