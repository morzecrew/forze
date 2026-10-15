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

from typing import Any

from forze.application.contracts.deps import GuardedWritePort
from forze.application.execution import (
    Deps,
    DepsModule,
    DepsRegistry,
    ExecutionContext,
    FrozenDeps,
)
from forze.base.exceptions import exc

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


# ....................... #


def unguarded(port: object) -> Any:
    """The real port behind a command accessor's :class:`GuardedWritePort`, for a test that
    checks its type or patches one of its methods; any other port is returned as it is.
    Under port spans or port logging that is still a proxy: ask a protocol question with
    :func:`~forze.application.contracts.deps.implements`, which sees through both.

    A command accessor never hands out the real port, so that a write is always judged by the
    operation making it. Outside a read-only operation this returns the one the guard
    delegates to, so a method patched onto it is the one the guard calls; inside one it
    refuses, as using the guard would.
    """

    if not isinstance(port, GuardedWritePort):
        return port

    if port.ctx.inv_ctx.is_read_only():
        raise exc.precondition(
            f"Cannot use command (write) port {port.key} in a read-only (QUERY) operation."
        )

    return port._target
