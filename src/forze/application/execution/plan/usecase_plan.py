"""Declarative per-operation middleware composition."""

from enum import StrEnum
from typing import Callable, Iterable, Self, Sequence, final

import attrs

from forze.application._logger import logger
from forze.application.execution.bucket import ALL_BUCKETS, Bucket
from forze.application.execution.capability_keys import CapabilityKey
from forze.base.descriptors import hybridmethod
from forze.base.errors import CoreError

from ..capabilities.chain import CapabilityChainBuilder
from ..capabilities.legacy_chain import LegacyChainBuilder
from ..capabilities.trace import CapabilityExecutionEvent
from ..context import ExecutionContext
from .builders import (
    effect_middleware_factory,
    finally_middleware_factory,
    guard_middleware_factory,
    on_failure_middleware_factory,
)
from .operation import OperationPlan
from .ordering import middleware_specs_for_usecase_tuple
from .report import ExecutionPlanReport, build_execution_plan_report
from .spec import (
    MiddlewareSpec,
    TransactionSpec,
    dispatch_edges_for_delegate_effect,
    frozenset_capability_keys,
)
from .steps import (
    PipelineEffectItem,
    PipelineGuardItem,
    normalize_pipeline_effect,
    normalize_pipeline_guard,
)
from .types import (
    WILDCARD,
    EffectFactory,
    FinallyFactory,
    GuardFactory,
    MiddlewareFactory,
    OnFailureFactory,
    OpKey,
    U,
)

# ----------------------- #


def _op_list(op: OpKey | list[OpKey]) -> list[OpKey]:
    return op if isinstance(op, list) else [op]


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class UsecasePlan:
    """Declarative plan for composing usecases per operation.

    Maps operation keys to :class:`OperationPlan`. Use ``*`` (wildcard) for
    defaults applied to all operations. :meth:`resolve` merges base and
    op-specific plans, then builds the middleware chain.

    Set :attr:`use_capability_engine` (or call :meth:`with_capability_engine`) to
    derive guard/effect order from declared capability keys inside each bucket.
    """

    ops: dict[str, OperationPlan] = attrs.field(factory=dict)
    use_capability_engine: bool = False

    # ....................... #

    def _base(self) -> OperationPlan:
        return self.ops.get(WILDCARD, OperationPlan())

    # ....................... #

    def _op(self, op: OpKey) -> OperationPlan:
        return self.ops.get(str(op), OperationPlan())

    # ....................... #

    def merged_operation_plan(self, op: OpKey) -> OperationPlan:
        """Merge the wildcard plan with the plan registered for ``op``."""

        return OperationPlan.merge(self._base(), self._op(op))

    # ....................... #

    def _put(self, op: OpKey, plan: OperationPlan) -> Self:
        new_ops = dict(self.ops)
        new_ops[str(op)] = plan

        return attrs.evolve(self, ops=new_ops)

    # ....................... #

    def _add(self, op: OpKey, bucket: Bucket, spec: MiddlewareSpec) -> Self:
        b = bucket.value

        logger.trace(
            "Adding middleware to usecase plan (op=%s, bucket=%s, priority=%s, factory_id=%s)",
            op,
            b,
            spec.priority,
            id(spec.factory),
        )

        cur = self._op(op)
        logger.trace("Current operation tx=%s", cur.tx)

        return self._put(op, cur.add(bucket, spec))

    # ....................... #

    def derived_dispatch_edges(self) -> frozenset[tuple[str, str]]:
        """Collect ``dispatch_edges`` from every middleware spec in this plan."""

        acc: set[tuple[str, str]] = set()

        for oplan in self.ops.values():
            for bucket in ALL_BUCKETS:
                for spec in getattr(oplan, bucket.value):
                    acc.update(spec.dispatch_edges)

        return frozenset(acc)

    # ....................... #

    def with_capability_engine(self, enabled: bool = True) -> Self:
        """Return a plan that enables or disables capability-driven guard/effect ordering."""

        return attrs.evolve(self, use_capability_engine=bool(enabled))

    # ....................... #

    def explain(self, op: OpKey) -> ExecutionPlanReport:
        """Return a static report of merged middleware and capability schedules for ``op``."""

        op_s = str(op)

        if op_s == WILDCARD or op_s.endswith(WILDCARD):
            raise CoreError("Explain on wildcard operation is not allowed")

        plan = OperationPlan.merge(self._base(), self._op(op_s))
        plan.validate()

        return build_execution_plan_report(
            plan=plan,
            op=op_s,
            use_capability_engine=self.use_capability_engine,
        )

    # ....................... #

    def tx(self, op: OpKey | list[OpKey], *, route: str | StrEnum) -> Self:
        out: Self = self

        for o in _op_list(op):
            logger.trace("Enabling transaction for operation '%s' (route=%s)", o, route)
            cur = out._op(o)

            out = out._put(o, attrs.evolve(cur, tx=TransactionSpec(route=route)))

        return out

    # ....................... #

    def no_tx(self, op: OpKey | list[OpKey]) -> Self:
        out: Self = self

        for o in _op_list(op):
            logger.trace("Disabling transaction for operation '%s'", o)
            cur = out._op(o)

            out = out._put(o, attrs.evolve(cur, tx=None))

        return out

    # ....................... #

    def _add_guard(
        self,
        op: OpKey | list[OpKey],
        bucket: Bucket,
        guard: GuardFactory,
        *,
        priority: int,
        requires: frozenset[str] | set[str] | Iterable[str | CapabilityKey] | None,
        provides: frozenset[str] | set[str] | Iterable[str | CapabilityKey] | None,
        step_label: str | None,
    ) -> Self:
        factory = guard_middleware_factory(guard)
        req = frozenset_capability_keys(requires)
        prov = frozenset_capability_keys(provides)

        out: Self = self

        for o in _op_list(op):
            out = out._add(
                o,
                bucket,
                MiddlewareSpec(
                    factory=factory,
                    priority=priority,
                    requires=req,
                    provides=prov,
                    step_label=step_label,
                ),
            )

        return out

    # ....................... #

    def before(
        self,
        op: OpKey | list[OpKey],
        guard: GuardFactory,
        *,
        priority: int = 0,
        requires: (
            frozenset[str] | set[str] | Iterable[str | CapabilityKey] | None
        ) = None,
        provides: (
            frozenset[str] | set[str] | Iterable[str | CapabilityKey] | None
        ) = None,
        step_label: str | None = None,
    ) -> Self:
        return self._add_guard(
            op,
            Bucket.outer_before,
            guard,
            priority=priority,
            requires=requires,
            provides=provides,
            step_label=step_label,
        )

    # ....................... #

    def before_pipeline(
        self,
        op: OpKey | list[OpKey],
        guards: Sequence[PipelineGuardItem],
        *,
        first_priority: int = 0,
    ) -> Self:
        out: Self = self

        for i, item in enumerate(guards):
            priority = first_priority - i * 10
            gf, req, prov, label = normalize_pipeline_guard(item)
            out = out.before(
                op,
                gf,
                priority=priority,
                requires=req,
                provides=prov,
                step_label=label,
            )

        return out

    # ....................... #

    def _add_effect(
        self,
        op: OpKey | list[OpKey],
        bucket: Bucket,
        effect: EffectFactory,
        *,
        priority: int,
        requires: frozenset[str] | set[str] | Iterable[str | CapabilityKey] | None,
        provides: frozenset[str] | set[str] | Iterable[str | CapabilityKey] | None,
        step_label: str | None,
    ) -> Self:
        req = frozenset_capability_keys(requires)
        prov = frozenset_capability_keys(provides)
        factory = effect_middleware_factory(effect)

        out: Self = self

        for o in _op_list(op):
            o_s = str(o)
            d_edges = dispatch_edges_for_delegate_effect((o_s,), effect)

            out = out._add(
                o,
                bucket,
                MiddlewareSpec(
                    factory=factory,
                    priority=priority,
                    dispatch_edges=d_edges,
                    requires=req,
                    provides=prov,
                    step_label=step_label,
                ),
            )

        return out

    # ....................... #

    def after(
        self,
        op: OpKey | list[OpKey],
        effect: EffectFactory,
        *,
        priority: int = 0,
        requires: (
            frozenset[str] | set[str] | Iterable[str | CapabilityKey] | None
        ) = None,
        provides: (
            frozenset[str] | set[str] | Iterable[str | CapabilityKey] | None
        ) = None,
        step_label: str | None = None,
    ) -> Self:
        return self._add_effect(
            op,
            Bucket.outer_after,
            effect,
            priority=priority,
            requires=requires,
            provides=provides,
            step_label=step_label,
        )

    # ....................... #

    def after_pipeline(
        self,
        op: OpKey | list[OpKey],
        effects: Sequence[PipelineEffectItem],
        *,
        first_priority: int = 0,
    ) -> Self:
        out: Self = self

        for i, item in enumerate(effects):
            priority = first_priority - i * 10
            ef, req, prov, label = normalize_pipeline_effect(item)
            out = out.after(
                op,
                ef,
                priority=priority,
                requires=req,
                provides=prov,
                step_label=label,
            )

        return out

    # ....................... #

    def wrap(
        self,
        op: OpKey | list[OpKey],
        middleware: MiddlewareFactory,
        *,
        priority: int = 0,
    ) -> Self:
        out: Self = self

        for o in _op_list(op):
            out = out._add(
                o,
                Bucket.outer_wrap,
                MiddlewareSpec(factory=middleware, priority=priority),
            )

        return out

    # ....................... #

    def wrap_pipeline(
        self,
        op: OpKey | list[OpKey],
        middlewares: Sequence[MiddlewareFactory],
        *,
        first_priority: int = 0,
    ) -> Self:
        out: Self = self

        for i, middleware in enumerate(middlewares):
            priority = first_priority - i * 10
            out = out.wrap(op, middleware, priority=priority)

        return out

    # ....................... #

    def outer_finally(
        self,
        op: OpKey | list[OpKey],
        hook: FinallyFactory,
        *,
        priority: int = 0,
    ) -> Self:
        factory = finally_middleware_factory(hook)
        out: Self = self

        for o in _op_list(op):
            out = out._add(
                o,
                Bucket.outer_finally,
                MiddlewareSpec(factory=factory, priority=priority),
            )

        return out

    # ....................... #

    def outer_finally_pipeline(
        self,
        op: OpKey | list[OpKey],
        hooks: Sequence[FinallyFactory],
        *,
        first_priority: int = 0,
    ) -> Self:
        out: Self = self

        for i, hook in enumerate(hooks):
            priority = first_priority - i * 10
            out = out.outer_finally(op, hook, priority=priority)

        return out

    # ....................... #

    def outer_on_failure(
        self,
        op: OpKey | list[OpKey],
        hook: OnFailureFactory,
        *,
        priority: int = 0,
    ) -> Self:
        factory = on_failure_middleware_factory(hook)
        out: Self = self

        for o in _op_list(op):
            out = out._add(
                o,
                Bucket.outer_on_failure,
                MiddlewareSpec(factory=factory, priority=priority),
            )

        return out

    # ....................... #

    def outer_on_failure_pipeline(
        self,
        op: OpKey | list[OpKey],
        hooks: Sequence[OnFailureFactory],
        *,
        first_priority: int = 0,
    ) -> Self:
        out: Self = self

        for i, hook in enumerate(hooks):
            priority = first_priority - i * 10
            out = out.outer_on_failure(op, hook, priority=priority)

        return out

    # ....................... #

    def in_tx_before(
        self,
        op: OpKey | list[OpKey],
        guard: GuardFactory,
        *,
        priority: int = 0,
        requires: (
            frozenset[str] | set[str] | Iterable[str | CapabilityKey] | None
        ) = None,
        provides: (
            frozenset[str] | set[str] | Iterable[str | CapabilityKey] | None
        ) = None,
        step_label: str | None = None,
    ) -> Self:
        return self._add_guard(
            op,
            Bucket.in_tx_before,
            guard,
            priority=priority,
            requires=requires,
            provides=provides,
            step_label=step_label,
        )

    # ....................... #

    def in_tx_before_pipeline(
        self,
        op: OpKey | list[OpKey],
        guards: Sequence[PipelineGuardItem],
        *,
        first_priority: int = 0,
    ) -> Self:
        out: Self = self

        for i, item in enumerate(guards):
            priority = first_priority - i * 10
            gf, req, prov, label = normalize_pipeline_guard(item)
            out = out.in_tx_before(
                op,
                gf,
                priority=priority,
                requires=req,
                provides=prov,
                step_label=label,
            )

        return out

    # ....................... #

    def in_tx_finally(
        self,
        op: OpKey | list[OpKey],
        hook: FinallyFactory,
        *,
        priority: int = 0,
    ) -> Self:
        factory = finally_middleware_factory(hook)
        out: Self = self

        for o in _op_list(op):
            out = out._add(
                o,
                Bucket.in_tx_finally,
                MiddlewareSpec(factory=factory, priority=priority),
            )

        return out

    # ....................... #

    def in_tx_finally_pipeline(
        self,
        op: OpKey | list[OpKey],
        hooks: Sequence[FinallyFactory],
        *,
        first_priority: int = 0,
    ) -> Self:
        out: Self = self

        for i, hook in enumerate(hooks):
            priority = first_priority - i * 10
            out = out.in_tx_finally(op, hook, priority=priority)

        return out

    # ....................... #

    def in_tx_on_failure(
        self,
        op: OpKey | list[OpKey],
        hook: OnFailureFactory,
        *,
        priority: int = 0,
    ) -> Self:
        factory = on_failure_middleware_factory(hook)
        out: Self = self

        for o in _op_list(op):
            out = out._add(
                o,
                Bucket.in_tx_on_failure,
                MiddlewareSpec(factory=factory, priority=priority),
            )

        return out

    # ....................... #

    def in_tx_on_failure_pipeline(
        self,
        op: OpKey | list[OpKey],
        hooks: Sequence[OnFailureFactory],
        *,
        first_priority: int = 0,
    ) -> Self:
        out: Self = self

        for i, hook in enumerate(hooks):
            priority = first_priority - i * 10
            out = out.in_tx_on_failure(op, hook, priority=priority)

        return out

    # ....................... #

    def in_tx_after(
        self,
        op: OpKey | list[OpKey],
        effect: EffectFactory,
        *,
        priority: int = 0,
        requires: (
            frozenset[str] | set[str] | Iterable[str | CapabilityKey] | None
        ) = None,
        provides: (
            frozenset[str] | set[str] | Iterable[str | CapabilityKey] | None
        ) = None,
        step_label: str | None = None,
    ) -> Self:
        return self._add_effect(
            op,
            Bucket.in_tx_after,
            effect,
            priority=priority,
            requires=requires,
            provides=provides,
            step_label=step_label,
        )

    # ....................... #

    def in_tx_after_pipeline(
        self,
        op: OpKey | list[OpKey],
        effects: Sequence[PipelineEffectItem],
        *,
        first_priority: int = 0,
    ) -> Self:
        out: Self = self

        for i, item in enumerate(effects):
            priority = first_priority - i * 10
            ef, req, prov, label = normalize_pipeline_effect(item)
            out = out.in_tx_after(
                op,
                ef,
                priority=priority,
                requires=req,
                provides=prov,
                step_label=label,
            )

        return out

    # ....................... #

    def in_tx_wrap(
        self,
        op: OpKey | list[OpKey],
        middleware: MiddlewareFactory,
        *,
        priority: int = 0,
    ) -> Self:
        out: Self = self

        for o in _op_list(op):
            out = out._add(
                o,
                Bucket.in_tx_wrap,
                MiddlewareSpec(factory=middleware, priority=priority),
            )

        return out

    # ....................... #

    def in_tx_wrap_pipeline(
        self,
        op: OpKey | list[OpKey],
        middlewares: Sequence[MiddlewareFactory],
        *,
        first_priority: int = 0,
    ) -> Self:
        out: Self = self

        for i, middleware in enumerate(middlewares):
            priority = first_priority - i * 10
            out = out.in_tx_wrap(op, middleware, priority=priority)

        return out

    # ....................... #

    def after_commit(
        self,
        op: OpKey | list[OpKey],
        effect: EffectFactory,
        *,
        priority: int = 0,
        requires: (
            frozenset[str] | set[str] | Iterable[str | CapabilityKey] | None
        ) = None,
        provides: (
            frozenset[str] | set[str] | Iterable[str | CapabilityKey] | None
        ) = None,
        step_label: str | None = None,
    ) -> Self:
        return self._add_effect(
            op,
            Bucket.after_commit,
            effect,
            priority=priority,
            requires=requires,
            provides=provides,
            step_label=step_label,
        )

    # ....................... #

    def after_commit_pipeline(
        self,
        op: OpKey | list[OpKey],
        effects: Sequence[PipelineEffectItem],
        *,
        first_priority: int = 0,
    ) -> Self:
        out: Self = self

        for i, item in enumerate(effects):
            priority = first_priority - i * 10
            ef, req, prov, label = normalize_pipeline_effect(item)
            out = out.after_commit(
                op,
                ef,
                priority=priority,
                requires=req,
                provides=prov,
                step_label=label,
            )

        return out

    # ....................... #

    def in_tx_pipeline(
        self,
        op: OpKey | list[OpKey],
        before: Sequence[PipelineGuardItem] | None = None,
        after: Sequence[PipelineEffectItem] | None = None,
        wrap: Sequence[MiddlewareFactory] | None = None,
        on_failure: Sequence[OnFailureFactory] | None = None,
        finally_hooks: Sequence[FinallyFactory] | None = None,
        *,
        first_priority: int = 0,
    ) -> Self:
        out: Self = self

        if before is not None:
            out = out.in_tx_before_pipeline(op, before, first_priority=first_priority)

        if finally_hooks is not None:
            out = out.in_tx_finally_pipeline(
                op, finally_hooks, first_priority=first_priority
            )

        if on_failure is not None:
            out = out.in_tx_on_failure_pipeline(
                op, on_failure, first_priority=first_priority
            )

        if wrap is not None:
            out = out.in_tx_wrap_pipeline(op, wrap, first_priority=first_priority)

        if after is not None:
            out = out.in_tx_after_pipeline(op, after, first_priority=first_priority)

        return out

    # ....................... #

    def outer_pipeline(
        self,
        op: OpKey | list[OpKey],
        before: Sequence[PipelineGuardItem] | None = None,
        after: Sequence[PipelineEffectItem] | None = None,
        wrap: Sequence[MiddlewareFactory] | None = None,
        on_failure: Sequence[OnFailureFactory] | None = None,
        finally_hooks: Sequence[FinallyFactory] | None = None,
        *,
        first_priority: int = 0,
    ) -> Self:
        out: Self = self

        if before is not None:
            out = out.before_pipeline(op, before, first_priority=first_priority)

        if wrap is not None:
            out = out.wrap_pipeline(op, wrap, first_priority=first_priority)

        if finally_hooks is not None:
            out = out.outer_finally_pipeline(
                op, finally_hooks, first_priority=first_priority
            )

        if on_failure is not None:
            out = out.outer_on_failure_pipeline(
                op, on_failure, first_priority=first_priority
            )

        if after is not None:
            out = out.after_pipeline(op, after, first_priority=first_priority)

        return out

    # ....................... #

    def resolve(
        self,
        op: OpKey,
        ctx: ExecutionContext,
        factory: Callable[[ExecutionContext], U],
        *,
        capability_execution_trace: list[CapabilityExecutionEvent] | None = None,
    ) -> U:
        """Build a composed usecase instance for an operation."""

        op_s = str(op)

        logger.debug("Resolving usecase plan")

        if op_s == WILDCARD or op_s.endswith(WILDCARD):
            raise CoreError("Resolve on wildcard operation is not allowed")

        plan = OperationPlan.merge(self._base(), self._op(op_s))
        plan.validate()

        outer_before = middleware_specs_for_usecase_tuple(plan, Bucket.outer_before)
        outer_wrap = middleware_specs_for_usecase_tuple(plan, Bucket.outer_wrap)
        outer_finally = middleware_specs_for_usecase_tuple(plan, Bucket.outer_finally)
        outer_on_failure = middleware_specs_for_usecase_tuple(
            plan, Bucket.outer_on_failure
        )
        outer_after = middleware_specs_for_usecase_tuple(plan, Bucket.outer_after)

        in_tx_before = middleware_specs_for_usecase_tuple(plan, Bucket.in_tx_before)
        in_tx_finally = middleware_specs_for_usecase_tuple(plan, Bucket.in_tx_finally)
        in_tx_on_failure = middleware_specs_for_usecase_tuple(
            plan, Bucket.in_tx_on_failure
        )
        in_tx_wrap = middleware_specs_for_usecase_tuple(plan, Bucket.in_tx_wrap)
        in_tx_after = middleware_specs_for_usecase_tuple(plan, Bucket.in_tx_after)

        after_commit = plan.build(Bucket.after_commit)

        logger.trace("Built plan for '%s' (tx=%s)", op_s, plan.tx)

        if self.use_capability_engine:
            chain = list(
                CapabilityChainBuilder(
                    ctx=ctx,
                    plan=plan,
                    capability_execution_trace=capability_execution_trace,
                ).build(
                    outer_before=outer_before,
                    outer_wrap=outer_wrap,
                    outer_finally=outer_finally,
                    outer_on_failure=outer_on_failure,
                    outer_after=outer_after,
                    in_tx_before=in_tx_before,
                    in_tx_finally=in_tx_finally,
                    in_tx_on_failure=in_tx_on_failure,
                    in_tx_wrap=in_tx_wrap,
                    in_tx_after=in_tx_after,
                    after_commit_specs=after_commit,
                )
            )

        else:
            chain = list(
                LegacyChainBuilder(ctx=ctx, plan=plan).build(
                    outer_before=outer_before,
                    outer_wrap=outer_wrap,
                    outer_finally=outer_finally,
                    outer_on_failure=outer_on_failure,
                    outer_after=outer_after,
                    in_tx_before=in_tx_before,
                    in_tx_finally=in_tx_finally,
                    in_tx_on_failure=in_tx_on_failure,
                    in_tx_wrap=in_tx_wrap,
                    in_tx_after=in_tx_after,
                    after_commit=after_commit,
                )
            )

        logger.trace("Constructed middleware chain with %s middleware(s)", len(chain))

        uc = factory(ctx)
        resolved = uc.with_middlewares(*chain)

        return resolved

    # ....................... #

    @hybridmethod
    def merge(  # type: ignore[misc]
        cls: type[Self],  # pyright: ignore[reportGeneralTypeIssues]
        *plans: Self,
    ) -> Self:
        """Merge multiple plans into a single aggregate plan."""

        acc: dict[str, OperationPlan] = {}
        use_engine = False

        for p in plans:
            use_engine = use_engine or p.use_capability_engine

            for op_k, pl in p.ops.items():
                cur = acc.get(op_k, OperationPlan())
                acc[op_k] = cur.merge(pl)

        return cls(ops=acc, use_capability_engine=use_engine)

    # ....................... #

    @merge.instancemethod
    def _merge_instance(  # pyright: ignore[reportUnusedFunction]
        self: Self,
        *plans: Self,
    ) -> Self:
        return type(self).merge(self, *plans)
