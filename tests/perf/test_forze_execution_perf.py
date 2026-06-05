"""Micro-benchmarks for the execution hot path: operation resolution and invocation.

Perf tier (``@pytest.mark.perf``): excluded from ``just test``; run via ``just perf``.
In-process only (no Docker).

Measures the per-scope resolved-operation cache
(:attr:`~forze.application.execution.runtime.ExecutionRuntime.cache_resolved_operations`):
``cache_operations=True`` builds an operation's hooks/middleware/handler once per scope
and reuses them; ``cache_operations=False`` rebuilds them on every ``resolve``.

Run only these benchmarks::

    just perf tests/perf/test_forze_execution_perf.py

Compare cached vs uncached resolution::

    just perf tests/perf/test_forze_execution_perf.py -k resolve

Compare end-to-end invocation (resolve + run)::

    just perf tests/perf/test_forze_execution_perf.py -k invoke

Save a baseline (optional)::

    just perf tests/perf/test_forze_execution_perf.py --benchmark-save=execution
"""

from __future__ import annotations

from typing import Any, Awaitable, Callable

import attrs
import pytest

from forze.application.contracts.execution import (
    BeforeStep,
    Handler,
    MiddlewareStep,
)
from forze.application.execution import Deps, ExecutionContext
from forze.application.execution.operations.planning import OperationPlan
from forze.application.execution.operations.registry import OperationRegistry
from forze.application.execution.operations.registry.registries import (
    FrozenOperationRegistry,
)
from tests.support.execution_context import frozen_deps_from_deps

# ----------------------- #

_OP = "projects.create"
_BEFORE_HOOKS = 3
_MIDDLEWARE = 2


@attrs.define(slots=True, kw_only=True, frozen=True)
class _EchoHandler(Handler[str, str]):
    async def __call__(self, args: str) -> str:
        return f"result:{args}"


def _before_factory(_ctx: ExecutionContext) -> Callable[[Any], Awaitable[None]]:
    async def _before(_args: Any) -> None:
        return None

    return _before


def _middleware_factory(
    _ctx: ExecutionContext,
) -> Callable[[Callable[[Any], Awaitable[Any]], Any], Awaitable[Any]]:
    async def _mw(
        next_: Callable[[Any], Awaitable[Any]],
        args: Any,
    ) -> Any:
        return await next_(args)

    return _mw


def _registry() -> FrozenOperationRegistry:
    """Registry for one op with a representative hook/middleware plan."""

    plan = (
        OperationPlan()
        .bind_outer()
        .before(
            *(
                BeforeStep(id=f"before.{i}", factory=_before_factory)
                for i in range(_BEFORE_HOOKS)
            )
        )
        .wrap(
            *(
                MiddlewareStep(id=f"mw.{i}", factory=_middleware_factory)
                for i in range(_MIDDLEWARE)
            )
        )
        .finish(deep=False)
    )

    return OperationRegistry(
        handlers={_OP: lambda _ctx: _EchoHandler()},
        plans={_OP: plan},
    ).freeze()


def _context(*, cache: bool) -> ExecutionContext:
    return ExecutionContext(
        deps=frozen_deps_from_deps(Deps()),
        cache_operations=cache,
    )


# ----------------------- #
# Resolution (resolve only)
# ----------------------- #


@pytest.mark.perf
def test_resolve_cache_on_benchmark(benchmark: Any) -> None:
    """Cached resolve: build once per scope, then per-op dict hit."""

    reg = _registry()
    ctx = _context(cache=True)

    benchmark(lambda: reg.resolve(_OP, ctx))


@pytest.mark.perf
def test_resolve_cache_off_benchmark(benchmark: Any) -> None:
    """Uncached resolve: rebuild hooks/middleware/handler every call."""

    reg = _registry()
    ctx = _context(cache=False)

    benchmark(lambda: reg.resolve(_OP, ctx))


# ----------------------- #
# Invocation (resolve + run)
# ----------------------- #


@pytest.mark.perf
async def test_invoke_cache_on_benchmark(async_benchmark: Any) -> None:
    """End-to-end resolve + run with the per-scope cache enabled."""

    reg = _registry()
    ctx = _context(cache=True)

    async def _run() -> str:
        return await reg.resolve(_OP, ctx)("x")

    await async_benchmark(_run)


@pytest.mark.perf
async def test_invoke_cache_off_benchmark(async_benchmark: Any) -> None:
    """End-to-end resolve + run rebuilding the plan every call."""

    reg = _registry()
    ctx = _context(cache=False)

    async def _run() -> str:
        return await reg.resolve(_OP, ctx)("x")

    await async_benchmark(_run)
