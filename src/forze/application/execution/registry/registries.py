from __future__ import annotations

from typing import TYPE_CHECKING, Any, Mapping, Self

import attrs

from forze.application.contracts.execution import DispatchStep, HandlerFactory
from forze.base.descriptors import hybridmethod
from forze.base.errors import CoreError
from forze.base.primitives import (
    DirectedAcyclicGraph,
    StrKey,
    StrKeyNamespace,
    StrKeySelector,
    str_key_selector,
)

from ..planning import FrozenOperationPlan, OperationPlan
from ..running import DispatchedOperation, OperationRunner, ResolvedOperation
from .binder import OperationRegistryBinder

if TYPE_CHECKING:
    from ..context import ExecutionContext

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class PlanPatch:
    """Plan patch keyed by a string key selector."""

    selector: StrKeySelector.Spec
    """Selector matching registered operation keys."""

    plan: OperationPlan
    """Partial plan merged for matching operations at freeze."""


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class OperationRegistry:
    """Registry for operations."""

    _handlers: Mapping[StrKey, HandlerFactory] = attrs.field(
        factory=dict[StrKey, HandlerFactory],
        alias="handlers",
    )
    """Handler factories for operations."""

    _plans: Mapping[StrKey, OperationPlan] = attrs.field(
        factory=dict[StrKey, OperationPlan],
        alias="plans",
    )
    """Execution plans for operations."""

    _patches: tuple[PlanPatch, ...] = attrs.field(factory=tuple, alias="patches")
    """Plan patches applied by selector at freeze."""

    # ....................... #

    def get_plans(self) -> dict[StrKey, OperationPlan]:
        """Read-only access to the plans."""

        return dict(self._plans)

    # ....................... #

    def get_patches(self) -> tuple[PlanPatch, ...]:
        """Read-only access to plan patches."""

        return self._patches

    # ....................... #

    def set_handler(
        self,
        op: StrKey,
        handler: HandlerFactory,
        *,
        override: bool = False,
        namespace: StrKeyNamespace | None = None,
    ) -> Self:
        """Set the handler factory for an operation."""

        if namespace is not None:
            op = namespace.key(op)

        if op in self._handlers and not override:
            raise CoreError(f"Handler factory already set for operation: {op}")

        new_handlers = dict(self._handlers)
        new_handlers[op] = handler

        return attrs.evolve(self, handlers=new_handlers)

    # ....................... #

    def set_handlers(
        self,
        handlers: Mapping[StrKey, HandlerFactory],
        *,
        override: bool = False,
        namespace: StrKeyNamespace | None = None,
    ) -> Self:
        """Set the handler factories for multiple operations."""

        new_handlers = dict(self._handlers)

        if namespace is not None:
            handlers = {namespace.key(op): handler for op, handler in handlers.items()}

        for op, handler in handlers.items():
            if op in new_handlers and not override:
                raise CoreError(f"Handler factory already set for operation: {op}")

            new_handlers[op] = handler

        return attrs.evolve(self, handlers=new_handlers)

    # ....................... #

    def bind(
        self,
        *ops: StrKey,
        namespace: StrKeyNamespace | None = None,
    ) -> OperationRegistryBinder:
        """Spawn operation registry binder (planner) for a set of operations."""

        ops_ = set(ops)

        if not ops_:
            raise CoreError("No operations provided")

        if namespace is not None:
            ops_ = {namespace.key(op) for op in ops_}

        return OperationRegistryBinder(parent=self, ops=ops_, patch_selector=None)

    # ....................... #

    def patch(self, selector: StrKeySelector.Spec) -> OperationRegistryBinder:
        """Spawn a binder that commits a plan patch for operations matching ``selector``.

        Selectors target absolute operation keys (as registered on handlers), not
        namespace-relative segments.
        """

        return OperationRegistryBinder(parent=self, ops=None, patch_selector=selector)

    # ....................... #

    def commit_patch(
        self,
        selector: StrKeySelector.Spec,
        plan: OperationPlan,
    ) -> Self:
        """Merge or append a plan patch for ``selector``."""

        patches = list(self._patches)

        for index, entry in enumerate(patches):
            if entry.selector == selector:
                patches[index] = PlanPatch(
                    selector=selector,
                    plan=entry.plan.merge(plan),
                )
                return attrs.evolve(self, patches=tuple(patches))

        patches.append(PlanPatch(selector=selector, plan=plan))

        return attrs.evolve(self, patches=tuple(patches))

    # ....................... #

    def _patch_indices_by_specificity(self) -> tuple[int, ...]:
        """Patch indices ordered by ascending specificity, then registration order."""

        indices = tuple(range(len(self._patches)))

        return tuple(
            sorted(
                indices,
                key=lambda i: (
                    str_key_selector.specificity(self._patches[i].selector),
                    i,
                ),
            ),
        )

    # ....................... #

    def _resolve_plan(self, op: str) -> OperationPlan:
        """Resolve the effective plan for an operation key."""

        plan = OperationPlan()

        for index in self._patch_indices_by_specificity():
            patch = self._patches[index]

            if str_key_selector.matches(patch.selector, op):
                plan = plan.merge(patch.plan)

        explicit = self._plans.get(op)

        if explicit is not None:
            plan = plan.merge(explicit)

        return plan

    # ....................... #

    def extend_plan(
        self,
        op: StrKey,
        plan: OperationPlan,
        *,
        namespace: StrKeyNamespace | None = None,
    ) -> Self:
        """Extend plan for an operation."""

        if namespace is not None:
            op = namespace.key(op)

        new_plans = self.get_plans()

        old_plan = new_plans.get(op, OperationPlan())
        new_plans[op] = old_plan.merge(plan)

        return attrs.evolve(self, plans=new_plans)

    # ....................... #

    def extend_plans(
        self,
        plans: Mapping[StrKey, OperationPlan],
        *,
        namespace: StrKeyNamespace | None = None,
    ) -> Self:
        """Extend plans for multiple operations."""

        if namespace is not None:
            plans = {namespace.key(op): plan for op, plan in plans.items()}

        new_plans = self.get_plans()

        for op, plan in plans.items():
            old_plan = new_plans.get(op, OperationPlan())
            new_plans[op] = old_plan.merge(plan)

        return attrs.evolve(self, plans=new_plans)

    # ....................... #

    @hybridmethod
    def merge(cls: type[Self], *registries: Self) -> Self:  # type: ignore[misc, override]
        """Merge multiple operation registries into a single registry."""

        merged_handlers: dict[StrKey, HandlerFactory] = {}
        merged_plans: dict[StrKey, OperationPlan] = {}
        merged_patches: list[PlanPatch] = []

        for reg in registries:
            handler_conflicts = set(map(str, merged_handlers.keys())) & set(
                map(str, reg._handlers.keys())
            )
            plan_conflicts = set(map(str, merged_plans.keys())) & set(
                map(str, reg._plans.keys())
            )

            if handler_conflicts:
                raise CoreError(f"Conflicting handler factories: {handler_conflicts}")

            if plan_conflicts:
                raise CoreError(f"Conflicting operation plans: {plan_conflicts}")

            for patch in reg._patches:
                if any(
                    existing.selector == patch.selector for existing in merged_patches
                ):
                    raise CoreError(
                        f"Conflicting operation plan patches: {patch.selector!r}"
                    )

                merged_patches.append(patch)

            merged_handlers.update(reg._handlers)
            merged_plans.update(reg._plans)

        return cls(
            handlers=merged_handlers,
            plans=merged_plans,
            patches=tuple(merged_patches),
        )

    # ....................... #

    @merge.instancemethod
    def _merge_instance(self: Self, *registries: Self) -> Self:  # type: ignore[misc, override]
        return type(self).merge(self, *registries)

    # ....................... #

    def _validate_dispatch_graph(self) -> None:
        """Validate the dispatch graph of the operation registry.

        Method ensures that there are no dispatch loops or deadends.
        """

        nodes = set(self._handlers.keys())
        edges: set[tuple[StrKey, StrKey]] = set()

        for op in nodes:
            p = self._resolve_plan(str(op))

            for d in p.iter_dispatch():
                if d not in nodes:
                    raise CoreError(f"Dispatch target {d} not found for operation {op}")

                edges.add((op, d))

        g = DirectedAcyclicGraph.from_edges(nodes, edges)
        g.validate()

    # ....................... #

    def freeze(self) -> FrozenOperationRegistry:
        """Freeze the operation registry."""

        self._validate_dispatch_graph()

        frozen_handlers = dict(self._handlers)
        frozen_plans: dict[StrKey, FrozenOperationPlan] = {}

        for op in frozen_handlers:
            frozen_plans[op] = self._resolve_plan(str(op)).freeze()

        return FrozenOperationRegistry(handlers=frozen_handlers, plans=frozen_plans)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class FrozenOperationRegistry:
    """Frozen operation registry."""

    handlers: Mapping[StrKey, HandlerFactory] = attrs.field(
        factory=dict[StrKey, HandlerFactory],
    )
    """Handler factories for operations."""

    plans: Mapping[StrKey, FrozenOperationPlan] = attrs.field(
        factory=dict[StrKey, FrozenOperationPlan],
    )
    """Execution plans for operations."""

    # ....................... #

    def _dispatch(
        self,
        step: DispatchStep,
        ctx: "ExecutionContext",
    ) -> DispatchedOperation[Any, Any]:
        resolved = self.resolve(step.target, ctx)

        return DispatchedOperation(resolved=resolved, mapper=step.mapper)

    # ....................... #

    def resolve(
        self,
        op: StrKey,
        ctx: "ExecutionContext",
    ) -> ResolvedOperation[Any, Any]:
        if op not in self.handlers:
            raise CoreError(f"Handler factory not found for operation: {op}")

        handler = self.handlers[op]
        plan = self.plans[op]

        resolved_plan = plan.resolve(ctx, self._dispatch)
        runner = OperationRunner(
            op=op,
            plan=resolved_plan,
            tx_runner=ctx.tx.scope,
            defer_after_commit=ctx.tx.run_or_defer,
        )

        return ResolvedOperation(op=op, handler=handler(ctx), runner=runner)
