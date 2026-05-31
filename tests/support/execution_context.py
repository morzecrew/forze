"""Helpers for building :class:`ExecutionContext` via freeze/resolve."""

from __future__ import annotations

from forze.application.execution import (
    Deps,
    DepsModule,
    DepsRegistry,
    ExecutionContext,
    FrozenDeps,
)

# ----------------------- #


def frozen_deps_from_deps(*registration: Deps) -> FrozenDeps:
    """Freeze registration blobs into a per-scope resolver."""

    return DepsRegistry.from_deps(*registration).freeze().resolve()


def frozen_deps_from_modules(*modules: DepsModule) -> FrozenDeps:
    """Freeze module output into a per-scope resolver."""

    return DepsRegistry.from_modules(*modules).freeze().resolve()


def context_from_deps(*registration: Deps) -> ExecutionContext:
    """Build an execution context from registration deps blobs."""

    return ExecutionContext(deps=frozen_deps_from_deps(*registration))


def context_from_modules(*modules: DepsModule) -> ExecutionContext:
    """Build an execution context from deps modules."""

    return ExecutionContext(deps=frozen_deps_from_modules(*modules))
