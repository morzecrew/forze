"""Build an :class:`~forze.application.execution.ExecutionContext` for a test.

Unit-testing a handler or operation means calling it against a context wired to in-memory adapters
instead of real ones. These helpers do exactly that — freeze a set of deps modules (e.g. a
``MockDepsModule``) into a resolver and hand back a ready context — so a test is::

    ctx = context_from_modules(MockDepsModule(state=MockState()))
    result = await my_handler(ctx)(args)

No runtime, lifecycle, or transport — just the context your ports resolve from. For a transaction,
open a scope on it: ``async with ctx.tx_ctx.scope("mock"): ...``.
"""

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


# ....................... #


def frozen_deps_from_modules(*modules: DepsModule) -> FrozenDeps:
    """Freeze module output into a per-scope resolver."""

    return DepsRegistry.from_modules(*modules).freeze().resolve()


# ....................... #


def context_from_deps(*registration: Deps) -> ExecutionContext:
    """Build an execution context from registration deps blobs."""

    return ExecutionContext(deps=frozen_deps_from_deps(*registration))


# ....................... #


def context_from_modules(*modules: DepsModule) -> ExecutionContext:
    """Build an execution context from deps modules (the usual entry — pass a ``MockDepsModule``)."""

    return ExecutionContext(deps=frozen_deps_from_modules(*modules))
