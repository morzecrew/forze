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

from datetime import datetime
from typing import Any, Awaitable, Callable
from uuid import UUID

import attrs
import pytest

from forze.base.primitives import utcnow, uuid7
from forze.domain.models import BaseDTO, Document, invariant

from forze.application.contracts.deps import DepKey
from forze.application.contracts.execution import (
    BeforeStep,
    ExecutionPipeline,
    Handler,
    MiddlewareStep,
)
from forze.application.execution import (
    Deps,
    DepsRegistry,
    ExecutionContext,
)
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


def _hookless_registry() -> FrozenOperationRegistry:
    """Registry for one op with no hooks/middleware (exercises the empty-scope fast path)."""

    plan = OperationPlan().bind_outer().finish(deep=False)

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


@pytest.mark.perf
async def test_invoke_hookless_benchmark(async_benchmark: Any) -> None:
    """End-to-end resolve + run for an op with no hooks/middleware.

    Exercises the empty-scope fast path that skips ``_run_scope_body`` (closures,
    empty graph/pipeline iterations, and the ``Outcome`` allocation). Compare with
    :func:`test_invoke_cache_on_benchmark` (3 before hooks + 2 middleware).
    """

    reg = _hookless_registry()
    ctx = _context(cache=True)

    async def _run() -> str:
        return await reg.resolve(_OP, ctx)("x")

    await async_benchmark(_run)


# ----------------------- #
# Dispatch overhead (pre-resolved: isolates the per-call dispatch chain from
# the resolve dict-hit, against a direct ``await handler(args)`` floor).
# ----------------------- #


@pytest.mark.perf
async def test_invoke_direct_baseline_benchmark(async_benchmark: Any) -> None:
    """Floor: ``await handler(args)`` with no Forze dispatch chain at all.

    The baseline every other invocation benchmark is measured against — the
    irreducible cost of awaiting the handler coroutine itself.
    """

    handler: Handler[str, str] = _EchoHandler()

    async def _run() -> str:
        return await handler("x")

    await async_benchmark(_run)


@pytest.mark.perf
async def test_dispatch_hookless_benchmark(async_benchmark: Any) -> None:
    """Pure empty-plan dispatch: pre-resolved op, no hooks/middleware.

    Resolved once up front so the per-call cost is *only* the dispatch chain
    (``__call__`` → ``_run`` → ``run_resolved_operation_plan`` →
    ``run_resolved_scope`` → ``transactional_core`` → handler) plus the
    active-operation / drain-gate / deadline-read overhead — no resolve dict
    hit. Compare against :func:`test_invoke_direct_baseline_benchmark` to size
    the empty-path frame overhead.
    """

    reg = _hookless_registry()
    ctx = _context(cache=True)
    resolved = reg.resolve(_OP, ctx)

    async def _run() -> str:
        return await resolved("x")

    await async_benchmark(_run)


@pytest.mark.perf
async def test_dispatch_hooked_benchmark(async_benchmark: Any) -> None:
    """Pure full-plan dispatch: pre-resolved op with 3 before hooks + 2 middleware.

    Same isolation as :func:`test_dispatch_hookless_benchmark` (no resolve cost
    per call), but exercises ``_run_scope_body`` and the per-call middleware
    wrap-fold. Difference vs the hookless dispatch benchmark is the hook/wrap
    machinery; difference vs the direct baseline is total dispatch overhead.
    """

    reg = _registry()
    ctx = _context(cache=True)
    resolved = reg.resolve(_OP, ctx)

    async def _run() -> str:
        return await resolved("x")

    await async_benchmark(_run)


# ----------------------- #
# Port resolution (resolve_configurable: ctx.<x>.query(spec))
# ----------------------- #

_PORT_KEY = DepKey[object]("perf.port")
_PORT_ROUTE = "perf.port"


@attrs.define(slots=True, kw_only=True, frozen=True)
class _PortSpec:
    name: str = _PORT_ROUTE


_PORT_SPEC = _PortSpec()


@attrs.define(slots=True, kw_only=True, frozen=True)
class _FakeGateway:
    """Stand-in for an integration gateway built per resolution."""

    client: object
    codec: object
    field_map: dict[str, str]


def _port_factory(_ctx: ExecutionContext, _spec: _PortSpec) -> _FakeGateway:
    # Representative per-resolution construction (codec/field-map/wiring).
    return _FakeGateway(
        client=object(),
        codec=object(),
        field_map={f"f{i}": f"col{i}" for i in range(8)},
    )


def _port_context(*, cache: bool) -> ExecutionContext:
    deps = (
        DepsRegistry.from_deps(Deps.routed({_PORT_KEY: {_PORT_ROUTE: _port_factory}}))
        .freeze()
        .resolve()
    )

    return ExecutionContext(deps=deps, cache_ports=cache)


@pytest.mark.perf
def test_resolve_port_cache_on_benchmark(benchmark: Any) -> None:
    """Cached port resolution: build the gateway once per scope, then dict hit."""

    ctx = _port_context(cache=True)

    benchmark(
        lambda: ctx.deps.resolve_configurable(
            ctx, _PORT_KEY, _PORT_SPEC, route=_PORT_ROUTE
        ),
    )


@pytest.mark.perf
def test_resolve_port_cache_off_benchmark(benchmark: Any) -> None:
    """Uncached port resolution: rebuild the gateway (codec/field-map/wiring) every call."""

    ctx = _port_context(cache=False)

    benchmark(
        lambda: ctx.deps.resolve_configurable(
            ctx, _PORT_KEY, _PORT_SPEC, route=_PORT_ROUTE
        ),
    )


# ----------------------- #
# Simple-dep resolution (resolve_simple: tx manager / domain dispatcher / tenant
# resolver). Now scope-memoized (keyed by ``(key, route)``) under the same
# ``cache_ports`` flag as configurable ports:
#
#   cache_off = factory re-run + allocation + bookkeeping on every access (old behavior)
#   cache_on  = memo hit (dict get) after the first build per scope (new behavior)
#   provide   = plain-instance dict-get floor, for reference
#
# The cache_off - cache_on gap is the realized per-access win.
# ----------------------- #

_SIMPLE_KEY = DepKey[object]("perf.simple")
_PLAIN_INSTANCE_KEY = DepKey[object]("perf.plain.instance")


@attrs.define(slots=True, kw_only=True, frozen=True)
class _Registry:
    """Stand-in for the shared handler registry a dispatcher captures."""


@attrs.define(slots=True, kw_only=True, frozen=True)
class _Dispatcher:
    """Representative simple-dep object, mirroring ``InProcessDomainEventDispatcher``.

    Captures a scope-shared registry plus the per-scope context — the shape that
    :mod:`forze.application.execution.domain.module` builds. With memoization it is
    built once per scope (matching that module's "built per scope" intent) instead of
    on every access.
    """

    registry: _Registry
    ctx: ExecutionContext


_SHARED_REGISTRY = _Registry()
_PLAIN_SINGLETON = object()


def _simple_context(*, cache: bool) -> ExecutionContext:
    """Context holding one allocating simple dep and one plain instance."""

    deps = (
        DepsRegistry.from_deps(
            Deps.plain(
                {
                    _SIMPLE_KEY: lambda ctx: _Dispatcher(
                        registry=_SHARED_REGISTRY, ctx=ctx
                    ),
                    _PLAIN_INSTANCE_KEY: _PLAIN_SINGLETON,
                }
            )
        )
        .freeze()
        .resolve()
    )

    return ExecutionContext(deps=deps, cache_ports=cache)


@pytest.mark.perf
def test_resolve_simple_cache_on_benchmark(benchmark: Any) -> None:
    """Memoized simple-dep resolution: build the dispatcher once per scope, then dict hit."""

    ctx = _simple_context(cache=True)

    benchmark(lambda: ctx.deps.resolve_simple(ctx, _SIMPLE_KEY))


@pytest.mark.perf
def test_resolve_simple_cache_off_benchmark(benchmark: Any) -> None:
    """Unmemoized simple-dep resolution: re-run the factory (+ allocation) every call.

    The pre-memoization behavior, kept as the before-number for the per-scope cache.
    """

    ctx = _simple_context(cache=False)

    benchmark(lambda: ctx.deps.resolve_simple(ctx, _SIMPLE_KEY))


@pytest.mark.perf
def test_provide_plain_instance_benchmark(benchmark: Any) -> None:
    """Floor: ``provide()`` dict-get of a pre-built instance, for reference."""

    ctx = _simple_context(cache=True)

    benchmark(lambda: ctx.deps.provide(_PLAIN_INSTANCE_KEY))


# ----------------------- #
# Middleware wrap composition (does prebuilding the composed callable pay off?).
#
# Per call, ``run_wrap_pipeline`` re-FOLDS the chain: for N middleware it allocates
# N ``_wrap_step`` closures, then awaits through them. Prebuilding would move the
# FOLD (composition) to once-per-scope; it cannot remove the EXECUTION (the N awaits
# still happen every call). So the addressable per-call saving is exactly the fold
# cost measured here (a cross-check against the amortized end-to-end path put the
# realized saving at ~1.05 µs/op for 3 middleware — the fold plus the
# ``run_wrap_pipeline`` coroutine wrapper that prebuilding also elides).
# ----------------------- #


def _passthrough_mw() -> Any:
    """One resolved passthrough middleware (``mw(next, args) -> await next(args)``)."""

    return _middleware_factory(_context(cache=True))


def _wrap_pipeline(n: int) -> ExecutionPipeline[Any]:
    """A resolved wrap pipeline of *n* passthrough middleware."""

    return ExecutionPipeline(steps=tuple(_passthrough_mw() for _ in range(n)))


async def _wrap_inner(args: Any) -> Any:
    """Trivial innermost callable (stands in for the transactional core / handler)."""

    return args


def _compose_wrap(
    pipeline: ExecutionPipeline[Any],
    inner: Any,
) -> Any:
    """Fold the wrap chain once — identical to the loop inside ``run_wrap_pipeline``."""

    steps = pipeline.steps

    if not steps:
        return inner

    call = inner

    for middleware in reversed(steps):

        async def _call(call_args: Any, _mw: Any = middleware, _nxt: Any = call) -> Any:
            return await _mw(_nxt, call_args)

        call = _call

    return call


@pytest.mark.perf
@pytest.mark.parametrize("n", [1, 3, 5])
def test_wrap_fold_cost_benchmark(benchmark: Any, n: int) -> None:
    """Sync per-call fold cost for N middleware — the exact saving prebuilding gives.

    Prebuilding moves this composition from per-call to once-per-scope, so this
    number IS the per-call win (it does not touch the N awaits, which remain).
    """

    pipeline = _wrap_pipeline(n)

    benchmark(lambda: _compose_wrap(pipeline, _wrap_inner))


# ----------------------- #
# Aggregate load conversion (AggregateRepository.load: read model -> domain).
#
# Today: ``domain_type.model_validate(read.model_dump())`` — dump the just-read read
# model to a dict, then full-validate (incl. domain invariants) into the domain type.
# The recursive dump is the cost (and it scales with nesting). Three strategies, on a
# flat and a nested aggregate:
#
#   roundtrip       = D.model_validate(read.model_dump())       -- current
#   from_attributes = D.model_validate(read, from_attributes=True)
#                     -- the robust replacement: reads each domain field by attribute
#                     (so @computed_field properties are picked up via getattr) and
#                     passes nested instances through (revalidate_instances='never'),
#                     skipping the recursive dump. KEEPS validation + invariants and is
#                     faithful incl. computed fields + aliases (verified == roundtrip;
#                     the ``read.__dict__`` shortcut instead CRASHES on computed fields).
#   construct       = D.model_construct(**read.__dict__)        -- floor: skips
#                     everything incl. invariants (unsafe; timing floor only).
#
#   roundtrip - from_attributes = the safe, faithful saving (the recommended change).
#   roundtrip - construct       = max possible saving (unsafe; for context only).
# ----------------------- #


class _AddrVO(BaseDTO):
    street: str
    city: str
    postcode: str


class _LineVO(BaseDTO):
    sku: str
    qty: int
    price: float


class _SmallRead(BaseDTO):
    id: UUID
    rev: int
    created_at: datetime
    last_update_at: datetime
    name: str
    amount: float
    active: bool


class _SmallAgg(Document):
    name: str
    amount: float
    active: bool

    @invariant
    def _amount_non_negative(self) -> None:
        if self.amount < 0:
            raise ValueError("amount must be >= 0")


class _NestedRead(BaseDTO):
    id: UUID
    rev: int
    created_at: datetime
    last_update_at: datetime
    name: str
    address: _AddrVO
    lines: list[_LineVO]


class _NestedAgg(Document):
    name: str
    address: _AddrVO
    lines: list[_LineVO]

    @invariant
    def _has_lines(self) -> None:
        if not self.lines:
            raise ValueError("must have at least one line")


def _agg_shape(shape: str) -> tuple[Any, type[Document]]:
    """Return a populated read-model instance and its target domain type."""

    if shape == "small":
        return (
            _SmallRead(
                id=uuid7(),
                rev=1,
                created_at=utcnow(),
                last_update_at=utcnow(),
                name="acme",
                amount=100.0,
                active=True,
            ),
            _SmallAgg,
        )

    return (
        _NestedRead(
            id=uuid7(),
            rev=1,
            created_at=utcnow(),
            last_update_at=utcnow(),
            name="acme",
            address=_AddrVO(street="1 Main", city="Springfield", postcode="00001"),
            lines=[
                _LineVO(sku=f"sku-{i}", qty=i + 1, price=1.5 * i) for i in range(5)
            ],
        ),
        _NestedAgg,
    )


@pytest.mark.perf
@pytest.mark.parametrize("shape", ["small", "nested"])
def test_aggregate_load_roundtrip_benchmark(benchmark: Any, shape: str) -> None:
    """Current path: ``D.model_validate(read.model_dump())`` (dump + full validate)."""

    read, domain_type = _agg_shape(shape)

    benchmark(lambda: domain_type.model_validate(read.model_dump()))


@pytest.mark.perf
@pytest.mark.parametrize("shape", ["small", "nested"])
def test_aggregate_load_from_attributes_benchmark(benchmark: Any, shape: str) -> None:
    """Robust replacement: ``D.model_validate(read, from_attributes=True)``.

    Skips the recursive dump, keeps validation + invariants, faithful incl. computed
    fields and aliases (verified equal to the roundtrip path).
    """

    read, domain_type = _agg_shape(shape)

    benchmark(lambda: domain_type.model_validate(read, from_attributes=True))


@pytest.mark.perf
@pytest.mark.parametrize("shape", ["small", "nested"])
def test_aggregate_load_construct_benchmark(benchmark: Any, shape: str) -> None:
    """Floor: ``D.model_construct(**read.__dict__)`` — skips validation + invariants."""

    read, domain_type = _agg_shape(shape)

    benchmark(lambda: domain_type.model_construct(**read.__dict__))


# In-process and deterministic: participates in the CI perf regression gate.
pytestmark = pytest.mark.perf_gate
