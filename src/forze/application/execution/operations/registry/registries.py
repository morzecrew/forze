from __future__ import annotations

from typing import TYPE_CHECKING, Any, Self

import attrs

from forze.application.contracts.execution import DispatchStep, HandlerFactory
from forze.base.descriptors import hybridmethod
from forze.base.exceptions import exc
from forze.base.primitives import (
    MappingConverter,
    StrKey,
    StrKeyMapping,
    StrKeyNamespace,
    StrKeySelector,
)

from ..descriptors import OperationCatalogEntry, OperationDescriptor
from ..planning import FrozenOperationPlan, OperationPlan
from ..run import DispatchedOperation, ResolvedOperation
from .binder import OperationRegistryBinder
from .merge import RegistryMerge
from .patch import PlanPatch
from .resolution import PlanResolution
from .validation import RegistryFreezeValidator

if TYPE_CHECKING:
    from ...context import ExecutionContext

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class OperationRegistry:
    """Registry for operations."""

    _handlers: StrKeyMapping[HandlerFactory] = attrs.field(
        factory=dict[StrKey, HandlerFactory],
        alias="handlers",
        converter=MappingConverter.to_str_key_frozen,  # type: ignore[misc]
    )
    """Handler factories for operations."""

    _plans: StrKeyMapping[OperationPlan] = attrs.field(
        factory=dict[StrKey, OperationPlan],
        alias="plans",
        converter=MappingConverter.to_str_key_frozen,  # type: ignore[misc]
    )
    """Execution plans for operations."""

    _descriptors: StrKeyMapping[OperationDescriptor] = attrs.field(
        factory=dict[StrKey, OperationDescriptor],
        alias="descriptors",
        converter=MappingConverter.to_str_key_frozen,  # type: ignore[misc]
    )
    """Catalog metadata for operations (interface-agnostic; optional per operation)."""

    _patches: tuple[PlanPatch, ...] = attrs.field(factory=tuple, alias="patches")
    """Plan patches applied by selector at freeze."""

    # ....................... #

    def get_plans(self) -> dict[StrKey, OperationPlan]:
        """Read-only access to the plans."""

        return dict(self._plans)

    # ....................... #

    def get_descriptors(self) -> dict[StrKey, OperationDescriptor]:
        """Read-only access to the catalog descriptors."""

        return dict(self._descriptors)

    # ....................... #

    def operation_keys(self) -> frozenset[StrKey]:
        """Return every registered operation key (one per handler factory).

        Useful for cross-cutting instrumentation that targets all operations — e.g.
        ``instrument_operations`` (OpenTelemetry).
        """

        return frozenset(self._handlers)

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
            raise exc.internal(f"Handler factory already set for operation: {op}")

        new_handlers = dict(self._handlers)
        new_handlers[op] = handler

        return attrs.evolve(self, handlers=new_handlers)

    # ....................... #

    def set_handlers(
        self,
        handlers: StrKeyMapping[HandlerFactory],
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
                raise exc.internal(f"Handler factory already set for operation: {op}")

            new_handlers[op] = handler

        return attrs.evolve(self, handlers=new_handlers)

    # ....................... #

    def set_descriptor(
        self,
        op: StrKey,
        descriptor: OperationDescriptor,
        *,
        override: bool = False,
        namespace: StrKeyNamespace | None = None,
    ) -> Self:
        """Set the catalog descriptor for an operation."""

        if namespace is not None:
            op = namespace.key(op)

        if op in self._descriptors and not override:
            raise exc.internal(f"Descriptor already set for operation: {op}")

        new_descriptors = dict(self._descriptors)
        new_descriptors[op] = descriptor

        return attrs.evolve(self, descriptors=new_descriptors)

    # ....................... #

    def set_descriptors(
        self,
        descriptors: StrKeyMapping[OperationDescriptor],
        *,
        override: bool = False,
        namespace: StrKeyNamespace | None = None,
    ) -> Self:
        """Set the catalog descriptors for multiple operations."""

        new_descriptors = dict(self._descriptors)

        if namespace is not None:
            descriptors = {
                namespace.key(op): descriptor for op, descriptor in descriptors.items()
            }

        for op, descriptor in descriptors.items():
            if op in new_descriptors and not override:
                raise exc.internal(f"Descriptor already set for operation: {op}")

            new_descriptors[op] = descriptor

        return attrs.evolve(self, descriptors=new_descriptors)

    # ....................... #

    def bind(
        self,
        *ops: StrKey,
        namespace: StrKeyNamespace | None = None,
    ) -> OperationRegistryBinder:
        """Spawn operation registry binder (planner) for a set of operations."""

        ops_ = set(ops)

        if not ops_:
            raise exc.internal("No operations provided")

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

    def _resolution(self) -> PlanResolution:
        return PlanResolution(plans=self._plans, patches=self._patches)

    # ....................... #

    def _resolve_plan(self, op: str) -> OperationPlan:
        """Resolve the effective plan for an operation key."""

        return self._resolution().resolve(op)

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
        plans: StrKeyMapping[OperationPlan],
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

        merged = RegistryMerge.merge(
            *(
                RegistryMerge(
                    handlers=reg._handlers,
                    plans=reg._plans,
                    descriptors=reg._descriptors,
                    patches=reg._patches,
                )
                for reg in registries
            ),
        )

        return cls(
            handlers=merged.handlers,
            plans=merged.plans,
            descriptors=merged.descriptors,
            patches=merged.patches,
        )

    # ....................... #

    @merge.instancemethod
    def _merge_instance(self: Self, *registries: Self) -> Self:  # type: ignore[misc, override]
        return type(self).merge(self, *registries)

    # ....................... #

    def freeze(self) -> FrozenOperationRegistry:
        """Freeze the operation registry."""

        resolution = self._resolution()
        RegistryFreezeValidator.validate_all(self._handlers, resolution)

        frozen_handlers = dict(self._handlers)
        frozen_plans: dict[StrKey, FrozenOperationPlan] = {}

        for op in frozen_handlers:
            frozen_plans[op] = resolution.resolve(str(op)).freeze()

        return FrozenOperationRegistry(
            handlers=frozen_handlers,
            plans=frozen_plans,
            descriptors=dict(self._descriptors),
        )


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class FrozenOperationRegistry:
    """Frozen operation registry."""

    handlers: StrKeyMapping[HandlerFactory] = attrs.field(
        factory=dict[StrKey, HandlerFactory],
        converter=MappingConverter.to_str_key_frozen,  # type: ignore[misc]
    )
    """Handler factories for operations."""

    plans: StrKeyMapping[FrozenOperationPlan] = attrs.field(
        factory=dict[StrKey, FrozenOperationPlan],
        converter=MappingConverter.to_str_key_frozen,  # type: ignore[misc]
    )
    """Execution plans for operations."""

    descriptors: StrKeyMapping[OperationDescriptor] = attrs.field(
        factory=dict[StrKey, OperationDescriptor],
        converter=MappingConverter.to_str_key_frozen,  # type: ignore[misc]
    )
    """Catalog metadata for operations (interface-agnostic; optional per operation)."""

    # ....................... #

    def catalog(self) -> dict[StrKey, OperationCatalogEntry]:
        """Join descriptors with each operation's read/write kind into a catalog.

        One entry per registered handler. Operations without a declared descriptor still
        appear (with ``descriptor=None``) so callers can see the full operation surface;
        a driving adapter decides which entries it actually exposes.
        """

        return {
            op: OperationCatalogEntry(
                op=op,
                kind=self.plans[op].kind,
                descriptor=self.descriptors.get(op),
            )
            for op in self.handlers
        }

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
        cached = ctx.cached_operation(op)

        if cached is not None:
            return cached

        if op not in self.handlers:
            raise exc.internal(f"Handler factory not found for operation: {op}")

        handler = self.handlers[op]
        plan = self.plans[op]

        resolved_plan = plan.resolve(ctx, self._dispatch)

        resolved = ResolvedOperation(
            op=op,
            handler=handler(ctx),
            plan=resolved_plan,
            tx_runner=ctx.tx_ctx.scope,
            defer_after_commit=ctx.tx_ctx.run_or_defer,
            inv_ctx=ctx.inv_ctx,
        )

        ctx.store_operation(op, resolved)

        return resolved
