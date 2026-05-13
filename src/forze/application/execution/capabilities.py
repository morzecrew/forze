"""Capability store, scheduling, and segment middleware for usecase plans."""

import inspect
from collections import defaultdict
from typing import Any, Callable, Literal, Protocol, TypeVar

import attrs

from forze.application._logger import logger
from forze.base.errors import CoreError

from .context import ExecutionContext
from .middleware import (
    Effect,
    EffectMiddleware,
    Guard,
    GuardMiddleware,
    Middleware,
    NextCall,
    TxMiddleware,
)

# ----------------------- #

ArgsT = TypeVar("ArgsT")
ResT = TypeVar("ResT")


class SchedulableCapabilitySpec(Protocol):
    """Structural type for middleware specs in capability scheduling and segments."""

    priority: int
    requires: frozenset[str]
    provides: frozenset[str]
    step_label: str | None
    factory: Callable[[ExecutionContext], Any]


_Sched = TypeVar("_Sched", bound=SchedulableCapabilitySpec)


@attrs.define(slots=True, frozen=True, kw_only=True)
class CapabilitySkip:
    """Return this from a guard or effect to skip without aborting the usecase."""

    reason: str | None = None


GuardSkip = CapabilitySkip
"""Backward-compatible alias for :class:`CapabilitySkip`."""


CapabilityTraceKind = Literal["guard", "effect", "after_commit"]
CapabilityTraceAction = Literal["ran", "skipped_missing", "skipped_return", "error"]


@attrs.define(slots=True, kw_only=True, frozen=True)
class CapabilityExecutionEvent:
    """One capability-segment step outcome (for tests and diagnostics)."""

    bucket: str
    label: str
    kind: CapabilityTraceKind
    action: CapabilityTraceAction
    detail: str | None = None


def _capability_step_label(spec: SchedulableCapabilitySpec, impl: object) -> str:
    if spec.step_label:
        return spec.step_label

    fact = spec.factory
    fn = getattr(fact, "__qualname__", None) or getattr(fact, "__name__", None)

    if isinstance(fn, str) and fn:
        return fn

    return type(impl).__qualname__


# ....................... #


@attrs.define(slots=True)
class CapabilityStore:
    """Tracks capability readiness for one usecase invocation (shared across segments)."""

    _ready: set[str] = attrs.field(factory=set, repr=False)
    _missing: set[str] = attrs.field(factory=set, repr=False)
    trace_events: list[CapabilityExecutionEvent] | None = attrs.field(
        default=None,
        repr=False,
    )

    def is_ready(self, keys: frozenset[str]) -> bool:
        if not keys:
            return True

        return all(k in self._ready and k not in self._missing for k in keys)

    def mark_success(self, keys: frozenset[str]) -> None:
        for k in keys:
            self._missing.discard(k)
            self._ready.add(k)

    def mark_missing(self, keys: frozenset[str]) -> None:
        for k in keys:
            self._ready.discard(k)
            self._missing.add(k)

    def record_execution(
        self,
        *,
        bucket: str,
        spec: SchedulableCapabilitySpec,
        impl: object,
        kind: CapabilityTraceKind,
        action: CapabilityTraceAction,
        detail: str | None = None,
    ) -> None:
        if self.trace_events is None:
            return

        self.trace_events.append(
            CapabilityExecutionEvent(
                bucket=bucket,
                label=_capability_step_label(spec, impl),
                kind=kind,
                action=action,
                detail=detail,
            )
        )

    @property
    def execution_trace(self) -> tuple[CapabilityExecutionEvent, ...]:
        """Read-only view of events recorded for this invocation (if tracing was enabled)."""

        if self.trace_events is None:
            return ()

        return tuple(self.trace_events)


# ....................... #


def schedule_capability_specs(
    specs: tuple[_Sched, ...],
    *,
    bucket: str,
) -> tuple[_Sched, ...]:
    """Return ``specs`` reordered for capability constraints (per-bucket graph).

    When every spec has empty ``requires`` and ``provides``, preserves the
    incoming order (callers must pass specs already sorted by execution order).

    :param specs: :class:`~forze.application.execution.plan.MiddlewareSpec` tuple.
    :param bucket: Bucket label for error messages.
    :raises CoreError: on missing providers, duplicate providers, or cycles.
    """

    if not specs:
        return specs

    n = len(specs)
    any_cap = any(s.requires or s.provides for s in specs)

    if not any_cap:
        return specs

    key_providers: dict[str, int] = {}

    for i, s in enumerate(specs):
        for k in s.provides:
            if k in key_providers:
                raise CoreError(
                    f"Capability {k!r} is provided by more than one step in bucket "
                    f"{bucket!r} (indices {key_providers[k]} and {i})"
                )

            key_providers[k] = i

    adj: defaultdict[int, set[int]] = defaultdict(set)
    indeg = [0] * n

    for j, sj in enumerate(specs):
        for k in sj.requires:
            if k not in key_providers:
                raise CoreError(
                    f"Bucket {bucket!r}: capability {k!r} is required by a step "
                    f"but no step in this bucket provides it"
                )

            i = key_providers[k]

            if i == j:
                raise CoreError(
                    f"Bucket {bucket!r}: step at index {j} both requires and provides {k!r}"
                )

            if j not in adj[i]:
                adj[i].add(j)
                indeg[j] += 1

    order: list[int] = []
    ready = [i for i in range(n) if indeg[i] == 0]
    ready.sort(key=lambda idx: (-specs[idx].priority, idx))

    while ready:
        u = ready.pop(0)
        order.append(u)

        for v in sorted(adj[u], key=lambda idx: (-specs[idx].priority, idx)):
            indeg[v] -= 1

            if indeg[v] == 0:
                ready.append(v)
                ready.sort(key=lambda idx: (-specs[idx].priority, idx))

    if len(order) != n:
        raise CoreError(
            f"Capability dependency graph in bucket {bucket!r} contains a cycle"
        )

    return tuple(specs[i] for i in order)


# ....................... #


async def _maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value

    return value


@attrs.define(slots=True, kw_only=True)
class CapabilityGuardSegmentMiddleware(Middleware[ArgsT, ResT]):
    """Runs multiple guards in capability order with a shared :class:`CapabilityStore`."""

    bucket: str
    store: CapabilityStore
    steps: tuple[tuple[Guard[ArgsT], SchedulableCapabilitySpec], ...]
    """``(guard, spec)`` pairs; ``spec`` matches :class:`SchedulableCapabilitySpec`."""

    async def __call__(self, next: NextCall[ArgsT, ResT], args: ArgsT) -> ResT:
        for guard, spec in self.steps:
            label = _capability_step_label(spec, guard)

            if not self.store.is_ready(spec.requires):
                logger.debug(
                    "Skipping guard (missing capability): bucket=%s label=%s",
                    self.bucket,
                    label,
                )

                self.store.record_execution(
                    bucket=self.bucket,
                    spec=spec,
                    impl=guard,
                    kind="guard",
                    action="skipped_missing",
                    detail=None,
                )

                continue

            logger.debug(
                "Running guard (capability segment): bucket=%s label=%s",
                self.bucket,
                label,
            )

            raw = guard(args)
            result = await _maybe_await(raw)

            if isinstance(result, CapabilitySkip):
                self.store.mark_missing(spec.provides)

                logger.debug(
                    "Guard skipped: bucket=%s label=%s reason=%s",
                    self.bucket,
                    label,
                    result.reason,
                )

                self.store.record_execution(
                    bucket=self.bucket,
                    spec=spec,
                    impl=guard,
                    kind="guard",
                    action="skipped_return",
                    detail=result.reason,
                )

            else:
                self.store.mark_success(spec.provides)

                self.store.record_execution(
                    bucket=self.bucket,
                    spec=spec,
                    impl=guard,
                    kind="guard",
                    action="ran",
                    detail=None,
                )

        return await next(args)


@attrs.define(slots=True, kw_only=True)
class CapabilityEffectSegmentMiddleware(Middleware[ArgsT, ResT]):
    """Runs multiple effects in capability order after ``next`` returns."""

    bucket: str
    store: CapabilityStore
    steps: tuple[tuple[Effect[ArgsT, ResT], SchedulableCapabilitySpec], ...]

    async def __call__(self, next: NextCall[ArgsT, ResT], args: ArgsT) -> ResT:
        res = await next(args)

        for effect, spec in self.steps:
            label = _capability_step_label(spec, effect)

            if not self.store.is_ready(spec.requires):
                logger.debug(
                    "Skipping effect (missing capability): bucket=%s label=%s",
                    self.bucket,
                    label,
                )

                self.store.record_execution(
                    bucket=self.bucket,
                    spec=spec,
                    impl=effect,
                    kind="effect",
                    action="skipped_missing",
                    detail=None,
                )

                continue

            logger.debug(
                "Running effect (capability segment): bucket=%s label=%s",
                self.bucket,
                label,
            )

            raw = effect(args, res)
            out = await _maybe_await(raw)

            if isinstance(out, CapabilitySkip):
                self.store.mark_missing(spec.provides)

                logger.debug(
                    "Effect skipped: bucket=%s label=%s reason=%s",
                    self.bucket,
                    label,
                    out.reason,
                )

                self.store.record_execution(
                    bucket=self.bucket,
                    spec=spec,
                    impl=effect,
                    kind="effect",
                    action="skipped_return",
                    detail=out.reason,
                )

            else:
                res = out
                self.store.mark_success(spec.provides)

                self.store.record_execution(
                    bucket=self.bucket,
                    spec=spec,
                    impl=effect,
                    kind="effect",
                    action="ran",
                    detail=None,
                )

        return res


def _resolve_guard_steps(
    ctx: ExecutionContext,
    specs: tuple[_Sched, ...],
    *,
    bucket: str,
) -> tuple[tuple[Guard[Any], _Sched], ...]:
    out: list[tuple[Guard[Any], _Sched]] = []

    for spec in specs:
        mw = spec.factory(ctx)

        if not isinstance(mw, GuardMiddleware):
            raise CoreError(
                f"Expected GuardMiddleware in capability bucket {bucket!r}, got {type(mw)}"
            )

        out.append((mw.guard, spec))

    return tuple(out)


def _resolve_effect_steps(
    ctx: ExecutionContext,
    specs: tuple[_Sched, ...],
    *,
    bucket: str,
) -> tuple[tuple[Effect[Any, Any], _Sched], ...]:
    out: list[tuple[Effect[Any, Any], _Sched]] = []

    for spec in specs:
        mw = spec.factory(ctx)

        if not isinstance(mw, EffectMiddleware):
            raise CoreError(
                f"Expected EffectMiddleware in capability bucket {bucket!r}, got {type(mw)}"
            )

        out.append((mw.effect, spec))

    return tuple(out)


def build_capability_middleware_chain(
    *,
    ctx: ExecutionContext,
    plan: Any,
    outer_before: tuple[Any, ...],
    outer_wrap: tuple[Any, ...],
    outer_finally: tuple[Any, ...],
    outer_on_failure: tuple[Any, ...],
    outer_after: tuple[Any, ...],
    in_tx_before: tuple[Any, ...],
    in_tx_finally: tuple[Any, ...],
    in_tx_on_failure: tuple[Any, ...],
    in_tx_wrap: tuple[Any, ...],
    in_tx_after: tuple[Any, ...],
    after_commit_specs: tuple[Any, ...],
    capability_execution_trace: list[CapabilityExecutionEvent] | None = None,
) -> tuple[Middleware[Any, Any], ...]:
    """Build the same middleware tuple as :meth:`UsecasePlan.resolve` with capability segments."""

    store = CapabilityStore(trace_events=capability_execution_trace)

    def seg_guards(bucket: str, specs: tuple[Any, ...]) -> Middleware[Any, Any]:
        ordered = schedule_capability_specs(specs, bucket=bucket)
        steps = _resolve_guard_steps(ctx, ordered, bucket=bucket)

        return CapabilityGuardSegmentMiddleware[Any, Any](
            bucket=bucket,
            store=store,
            steps=steps,
        )

    def seg_effects(bucket: str, specs: tuple[Any, ...]) -> Middleware[Any, Any]:
        ordered = schedule_capability_specs(specs, bucket=bucket)
        steps = _resolve_effect_steps(ctx, ordered, bucket=bucket)

        return CapabilityEffectSegmentMiddleware[Any, Any](
            bucket=bucket,
            store=store,
            steps=steps,
        )

    after_commit_ordered = schedule_capability_specs(
        after_commit_specs,
        bucket="after_commit",
    )

    after_commit_effects: list[Effect[Any, Any]] = []

    for s in after_commit_ordered:
        mw = s.factory(ctx)

        if not isinstance(mw, EffectMiddleware):
            raise CoreError(f"Expected EffectMiddleware, got {type(mw)}")

        after_commit_effects.append(mw.effect)

    after_commit_tuple = tuple(after_commit_effects)

    async def _after_commit_capability_runner(
        args: Any,
        res: Any,
        *,
        _effects: tuple[Effect[Any, Any], ...] = after_commit_tuple,
        _specs: tuple[SchedulableCapabilitySpec, ...] = after_commit_ordered,
        _store: CapabilityStore = store,
    ) -> Any:
        for eff, spec in zip(_effects, _specs, strict=True):
            label = _capability_step_label(spec, eff)

            if not _store.is_ready(spec.requires):
                logger.debug(
                    "Skipping after_commit effect (missing capability): label=%s",
                    label,
                )

                _store.record_execution(
                    bucket="after_commit",
                    spec=spec,
                    impl=eff,
                    kind="after_commit",
                    action="skipped_missing",
                    detail=None,
                )

                continue

            logger.debug(
                "Running after_commit effect: label=%s",
                label,
            )

            raw = eff(args, res)
            out = await _maybe_await(raw)

            if isinstance(out, CapabilitySkip):
                _store.mark_missing(spec.provides)

                _store.record_execution(
                    bucket="after_commit",
                    spec=spec,
                    impl=eff,
                    kind="after_commit",
                    action="skipped_return",
                    detail=out.reason,
                )

            else:
                res = out
                _store.mark_success(spec.provides)

                _store.record_execution(
                    bucket="after_commit",
                    spec=spec,
                    impl=eff,
                    kind="after_commit",
                    action="ran",
                    detail=None,
                )

        return res

    chain: list[Middleware[Any, Any]] = []

    if outer_before:
        chain.append(seg_guards("outer_before", outer_before))

    chain.extend(s.factory(ctx) for s in outer_wrap)
    chain.extend(s.factory(ctx) for s in outer_finally)
    chain.extend(s.factory(ctx) for s in outer_on_failure)

    if plan.tx is not None:
        tx = TxMiddleware[Any, Any](ctx=ctx, route=plan.tx.route)

        if after_commit_tuple:
            tx = tx.with_after_commit(_after_commit_capability_runner)

        chain.append(tx)

        if in_tx_before:
            chain.append(seg_guards("in_tx_before", in_tx_before))

        chain.extend(s.factory(ctx) for s in in_tx_finally)
        chain.extend(s.factory(ctx) for s in in_tx_on_failure)
        chain.extend(s.factory(ctx) for s in in_tx_wrap)

        if in_tx_after:
            chain.append(seg_effects("in_tx_after", in_tx_after))

    elif after_commit_specs:
        raise CoreError(
            "after_commit middlewares present but transaction is disabled for this operation"
        )

    if outer_after:
        chain.append(seg_effects("outer_after", outer_after))

    return tuple(chain)
