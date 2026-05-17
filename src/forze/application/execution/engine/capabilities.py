"""Capability scheduling, runtime execution, and tracing."""

from __future__ import annotations

from collections import defaultdict
from typing import Any, Literal, TypeVar

import attrs

from forze.application._logger import logger
from forze.base.asyncio import maybe_await
from forze.base.errors import CoreError

from ..context import ExecutionContext
from ..middleware import (  # type: ignore[import-not-found]
    GuardMiddleware,
    Middleware,
    NextCall,
    Skip,
    SuccessHookMiddleware,
    ensure_schedulable_control,
)
from ..plan.spec import MiddlewareSpec

# ----------------------- #

ArgsT = TypeVar("ArgsT")
ResT = TypeVar("ResT")

CapabilityTraceKind = Literal["guard", "success_hook"]
CapabilityTraceAction = Literal["ran", "skipped_missing", "skipped_return", "error"]

# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class CapabilityExecutionEvent:
    """One capability step outcome for tests and diagnostics."""

    stage: str
    label: str
    kind: CapabilityTraceKind
    action: CapabilityTraceAction
    detail: str | None = None


# ....................... #


def capability_step_label(spec: MiddlewareSpec, impl: object) -> str:
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
    """Tracks capability readiness across one usecase invocation."""

    _ready: set[str] = attrs.field(factory=set, repr=False)
    _missing: set[str] = attrs.field(factory=set, repr=False)
    trace_events: list[CapabilityExecutionEvent] | None = attrs.field(
        default=None,
        repr=False,
    )

    # ....................... #

    def is_ready(self, keys: frozenset[str]) -> bool:
        if not keys:
            return True

        return all(key in self._ready and key not in self._missing for key in keys)

    # ....................... #

    def mark_success(self, keys: frozenset[str]) -> None:
        for key in keys:
            self._missing.discard(key)
            self._ready.add(key)

    # ....................... #

    def mark_missing(self, keys: frozenset[str]) -> None:
        for key in keys:
            self._ready.discard(key)
            self._missing.add(key)

    # ....................... #

    def record_execution(
        self,
        *,
        stage: str,
        spec: MiddlewareSpec,
        impl: object,
        kind: CapabilityTraceKind,
        action: CapabilityTraceAction,
        detail: str | None = None,
    ) -> None:
        if self.trace_events is None:
            return

        self.trace_events.append(
            CapabilityExecutionEvent(
                stage=stage,
                label=capability_step_label(spec, impl),
                kind=kind,
                action=action,
                detail=detail,
            )
        )

    # ....................... #

    @property
    def execution_trace(self) -> tuple[CapabilityExecutionEvent, ...]:
        if self.trace_events is None:
            return ()

        return tuple(self.trace_events)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class CapabilityResolvedStep:
    spec: MiddlewareSpec
    impl: object
    kind: CapabilityTraceKind
    invoke: Any


# ....................... #


def schedule_capability_specs(
    specs: tuple[MiddlewareSpec, ...],
    *,
    stage: str,
) -> tuple[MiddlewareSpec, ...]:
    if not specs:
        return specs

    if not any(spec.requires or spec.provides for spec in specs):
        return specs

    key_providers: dict[str, int] = {}

    for idx, spec in enumerate(specs):
        for key in spec.provides:
            if key in key_providers:
                raise CoreError(
                    f"Capability {key!r} is provided by more than one step in stage "
                    f"{stage!r} (indices {key_providers[key]} and {idx})",
                )

            key_providers[key] = idx

    adj: defaultdict[int, set[int]] = defaultdict(set)
    indeg = [0] * len(specs)

    for consumer_idx, consumer in enumerate(specs):
        for key in consumer.requires:
            if key not in key_providers:
                raise CoreError(
                    f"Stage {stage!r}: capability {key!r} is required by a step "
                    "but no step in this stage provides it",
                )

            provider_idx = key_providers[key]

            if provider_idx == consumer_idx:
                raise CoreError(
                    f"Stage {stage!r}: step at index {consumer_idx} both requires and provides {key!r}",
                )

            if consumer_idx not in adj[provider_idx]:
                adj[provider_idx].add(consumer_idx)
                indeg[consumer_idx] += 1

    order: list[int] = []
    ready = [idx for idx in range(len(specs)) if indeg[idx] == 0]
    ready.sort(key=lambda idx: (-specs[idx].priority, idx))

    while ready:
        current = ready.pop(0)
        order.append(current)

        for nxt in sorted(adj[current], key=lambda idx: (-specs[idx].priority, idx)):
            indeg[nxt] -= 1
            if indeg[nxt] == 0:
                ready.append(nxt)
                ready.sort(key=lambda idx: (-specs[idx].priority, idx))

    if len(order) != len(specs):
        raise CoreError(
            f"Capability dependency graph in stage {stage!r} contains a cycle",
        )

    return tuple(specs[idx] for idx in order)


# ....................... #


def execution_ordered_specs(
    specs: tuple[MiddlewareSpec, ...],
    *,
    stage: str,
) -> tuple[MiddlewareSpec, ...]:
    return schedule_capability_specs(specs, stage=stage)


# ....................... #


def resolve_capability_steps(
    ctx: ExecutionContext,
    specs: tuple[MiddlewareSpec, ...],
    *,
    stage: str,
    kind: CapabilityTraceKind,
) -> tuple[CapabilityResolvedStep, ...]:
    out: list[CapabilityResolvedStep] = []

    for spec in specs:
        mw = spec.factory(ctx)

        if kind == "guard":
            if not isinstance(mw, GuardMiddleware):
                raise CoreError(
                    f"Expected GuardMiddleware in capability stage {stage!r}, got {type(mw)}",
                )

            async def invoke_guard(
                args: Any,
                _result: Any,
                *,
                guard: Any = mw.guard,
            ) -> None | Skip:
                return ensure_schedulable_control(
                    await maybe_await(guard(args)),
                    kind="Guard",
                )

            out.append(
                CapabilityResolvedStep(
                    spec=spec,
                    impl=mw.guard,
                    kind=kind,
                    invoke=invoke_guard,
                )
            )
            continue

        if not isinstance(mw, SuccessHookMiddleware):
            raise CoreError(
                f"Expected SuccessHookMiddleware in capability stage {stage!r}, got {type(mw)}",
            )

        async def invoke_hook(
            args: Any,
            result: Any,
            *,
            hook: Any = mw.hook,
        ) -> None | Skip:
            return ensure_schedulable_control(
                await maybe_await(hook(args, result)),
                kind="Success hook",
            )

        out.append(
            CapabilityResolvedStep(
                spec=spec,
                impl=mw.hook,
                kind=kind,
                invoke=invoke_hook,
            )
        )

    return tuple(out)


# ....................... #


@attrs.define(slots=True, kw_only=True)
class CapabilityStageMiddleware(Middleware[ArgsT, ResT]):  # type: ignore[misc]
    """Runs multiple capability-aware steps within one execution stage."""

    stage: str
    kind: CapabilityTraceKind
    store: CapabilityStore
    steps: tuple[CapabilityResolvedStep, ...]

    # ....................... #

    async def _run(self, args: ArgsT, result: ResT | None) -> None:
        for step in self.steps:
            label = capability_step_label(step.spec, step.impl)

            if not self.store.is_ready(step.spec.requires):
                logger.debug(
                    "Skipping capability step (missing capability): stage=%s label=%s",
                    self.stage,
                    label,
                )
                self.store.record_execution(
                    stage=self.stage,
                    spec=step.spec,
                    impl=step.impl,
                    kind=step.kind,
                    action="skipped_missing",
                    detail=None,
                )
                continue

            logger.debug(
                "Running capability step: stage=%s label=%s",
                self.stage,
                label,
            )

            try:
                out = await step.invoke(args, result)
            except Exception as exc:
                self.store.record_execution(
                    stage=self.stage,
                    spec=step.spec,
                    impl=step.impl,
                    kind=step.kind,
                    action="error",
                    detail=type(exc).__qualname__,
                )
                raise

            if isinstance(out, Skip):
                self.store.mark_missing(step.spec.provides)
                self.store.record_execution(
                    stage=self.stage,
                    spec=step.spec,
                    impl=step.impl,
                    kind=step.kind,
                    action="skipped_return",
                    detail=out.reason,
                )
            else:
                self.store.mark_success(step.spec.provides)
                self.store.record_execution(
                    stage=self.stage,
                    spec=step.spec,
                    impl=step.impl,
                    kind=step.kind,
                    action="ran",
                    detail=None,
                )

    # ....................... #

    async def __call__(self, next: NextCall[ArgsT, ResT], args: ArgsT) -> ResT:
        if self.kind == "guard":
            await self._run(args, None)
            return await next(args)

        result = await next(args)
        await self._run(args, result)

        return result


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class CapabilityAfterCommitRunner:
    """Runs after-commit capability hooks with shared store and tracing."""

    store: CapabilityStore
    steps: tuple[CapabilityResolvedStep, ...]
    stage_label: str = "after_commit"
    _after_commit_batch: bool = attrs.field(default=True, repr=False)

    # ....................... #

    async def __call__(self, args: Any, result: Any) -> None:
        mw = CapabilityStageMiddleware[Any, Any](
            stage=self.stage_label,
            kind="success_hook",
            store=self.store,
            steps=self.steps,
        )

        async def done(_args: Any) -> Any:
            return result

        await mw(done, args)
