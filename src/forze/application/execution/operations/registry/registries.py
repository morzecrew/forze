from __future__ import annotations

from typing import TYPE_CHECKING, Any, Self

import attrs

from forze.application._logger import logger
from forze.application.contracts.execution import DispatchStep, OperationHandlerFactory
from forze.base.descriptors import hybridmethod
from forze.base.exceptions import exc
from forze.base.primitives import (
    MappingConverter,
    StrKey,
    StrKeyMapping,
    StrKeyNamespace,
    StrKeySelector,
    stable_payload_fingerprint,
    str_key_selector,
)

from ..descriptors import OperationCatalogEntry, OperationDescriptor
from ..planning import FrozenOperationPlan, OperationKind, OperationPlan
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

    _handlers: StrKeyMapping[OperationHandlerFactory] = attrs.field(
        factory=dict[StrKey, OperationHandlerFactory],
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
        handler: OperationHandlerFactory,
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
        handlers: StrKeyMapping[OperationHandlerFactory],
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

    def register(
        self,
        op: StrKey,
        handler: OperationHandlerFactory,
        *,
        descriptor: OperationDescriptor | None = None,
        override: bool = False,
        namespace: StrKeyNamespace | None = None,
    ) -> OperationRegistryBinder:
        """Register a handler (and optionally its catalog descriptor) in one step.

        Returns the same binder :meth:`bind` returns, so plan binding chains
        naturally::

            registry = (
                registry.register("notes.get", factory, descriptor=descriptor)
                .as_query()
                .finish()
            )

        Equivalent to ``set_handler`` + ``set_descriptor`` + ``bind`` — but keeps the
        handler and its catalog metadata in one statement, so an operation cannot end
        up registered yet invisible to catalog-driven surfaces (generated FastAPI
        routes, MCP tools) because its descriptor was forgotten. Call ``.finish()``
        (after any plan steps) to get the updated registry back.
        """

        if namespace is not None:
            op = namespace.key(op)

        registry = self.set_handler(op, handler, override=override)

        if descriptor is not None:
            registry = registry.set_descriptor(op, descriptor, override=override)

        return registry.bind(op)

    # ....................... #

    def patch(
        self,
        selector: StrKeySelector.Spec,
        *,
        namespace: StrKeyNamespace | None = None,
    ) -> OperationRegistryBinder:
        """Spawn a binder that commits a plan patch for operations matching *selector*.

        Selectors target absolute operation keys (as registered on handlers). Pass
        *namespace* to author the selector in namespace-relative terms — it is scoped
        to keys under that namespace and matched against the relative remainder,
        mirroring how ``bind``/``set_handler`` accept a namespace. This lets a
        sub-registry write ``patch(all_keys(), namespace=ns)`` to mean "everything
        *I* contribute" and remount cleanly under a merge.

        Args:
            selector (StrKeySelector.Spec): Selector choosing the operations to patch.
            namespace (StrKeyNamespace | None): When given, scopes *selector* to that
                namespace and matches it against the namespace-relative key.

        Returns:
            OperationRegistryBinder: A binder whose plan steps become the patch.
        """

        if namespace is not None:
            selector = str_key_selector.in_namespace(namespace, selector)

        return OperationRegistryBinder(parent=self, ops=None, patch_selector=selector)

    # ....................... #

    def commit_patch(
        self,
        selector: StrKeySelector.Spec,
        plan: OperationPlan,
        *,
        namespace: StrKeyNamespace | None = None,
    ) -> Self:
        """Merge or append a plan patch for *selector*, returning a new registry.

        When a patch with the same selector already exists, *plan* is merged into it;
        otherwise the patch is appended. Pass *namespace* to scope the selector to a
        namespace and match it against the namespace-relative key (see :meth:`patch`).

        Args:
            selector (StrKeySelector.Spec): Selector choosing the operations to patch.
            plan (OperationPlan): Plan steps merged into the matched operations' plans
                at freeze.
            namespace (StrKeyNamespace | None): When given, scopes *selector* to that
                namespace and matches it against the namespace-relative key.

        Returns:
            Self: A new registry with the patch merged or appended.
        """

        if namespace is not None:
            selector = str_key_selector.in_namespace(namespace, selector)

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

    def materialize_patches(self, *selectors: StrKeySelector.Spec) -> Self:
        """Resolve plan patches into per-operation plans and drop them from the registry.

        Each matched patch is merged (in specificity order) into the explicit plan
        of every operation it selects, then removed from the registry's live
        patches. The result is an editable registry with no late-bound patches for
        those selectors — only concrete per-operation plans — so a later
        :meth:`merge` carries materialized plans instead of selectors that could
        match a sibling registry's operations.

        With no arguments, *all* patches are materialized; pass one or more patch
        selectors to materialize only those, leaving the rest live. A selector
        passed here that is not a registered patch, or a materialized patch that
        matches no operation, is a configuration error — materializing asserts the
        patch is local and complete.

        Mental model: a **live** patch is late-bound ("apply wherever this lands");
        a **materialized** patch is early-bound ("settled here"). The registry
        boundary is where you choose binding time.

        Two caveats. (1) Materialized steps are merged into the explicit plan, which
        ``freeze`` always applies *last*; relative to a future lower-specificity
        patch this reorders them, so materialize is cleanest on a leaf registry or
        for order-orthogonal steps (deadlines, independent hooks). (2) Materializing
        a child's own patches does **not** make its operations immune to a parent's
        later broad patch — they remain in the handler set and still match.

        Args:
            *selectors (StrKeySelector.Spec): Patch selectors to materialize; with
                none given, every live patch is materialized.

        Returns:
            Self: A new registry whose matched patches are folded into per-operation
            plans and dropped from the live patch set.

        Raises:
            CoreException: If a passed selector is not a registered patch, or a
                materialized patch matches no operation (both configuration errors).
        """

        if not self._patches:
            if selectors:
                raise exc.configuration(
                    "materialize_patches called with selectors but the registry "
                    "has no plan patches"
                )

            return self

        targets = set(selectors)

        if targets:
            present = {patch.selector for patch in self._patches}

            if missing := [sel for sel in targets if sel not in present]:
                raise exc.configuration(
                    f"No plan patch found for selectors: {missing!r}"
                )

        selected = [
            patch for patch in self._patches if not targets or patch.selector in targets
        ]
        remaining = tuple(
            patch
            for patch in self._patches
            if targets and patch.selector not in targets
        )

        # Orphan guard: materializing asserts the patch is local and complete.
        for patch in selected:
            if not any(
                str_key_selector.matches(patch.selector, str(op))
                for op in self._handlers
            ):
                raise exc.configuration(
                    "Orphan plan patch: selector "
                    f"{patch.selector!r} matches no registered operations"
                )

        resolution = PlanResolution(plans={}, patches=tuple(selected))
        new_plans = self.get_plans()

        for op in self._handlers:
            op_str = str(op)

            if not any(
                str_key_selector.matches(patch.selector, op_str) for patch in selected
            ):
                continue

            materialized = resolution.resolve(op_str)
            old_plan = new_plans.get(op)
            new_plans[op] = (
                old_plan.merge(materialized) if old_plan is not None else materialized
            )

        return attrs.evolve(self, plans=new_plans, patches=remaining)

    # ....................... #

    def _resolution(self) -> PlanResolution:
        """Build a plan resolution from the registry's current plans and patches.

        Returns:
            PlanResolution: Snapshot pairing the registry's plans with its live
            patches, ready for freeze-time resolution.
        """

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
    def merge(  # type: ignore[misc, override]
        cls: type[Self],  # type: ignore[misc, override]
        *registries: Self,
        override: bool = False,
        cross_registry: bool = False,
    ) -> Self:
        """Merge multiple operation registries into a single registry.

        Registries are expected to be *disjoint*: a duplicate operation key (handler,
        plan, or descriptor) or duplicate patch selector raises a configuration error
        naming the colliding keys. Pass ``override=True`` to explicitly let later
        registries replace earlier entries instead.

        Plan patches are late-bound, so a patch authored in one registry can reach
        another's operations once they share the merged key set. That cross-registry
        reach is fail-closed: the merge raises naming the selectors and operations
        unless you scope the patch (``patch(selector, namespace=ns)``), settle it
        (:meth:`materialize_patches`) before merging, or pass ``cross_registry=True``
        to allow it explicitly. A top-level policy patch applied *after* the merge is
        unaffected — it never travels through ``merge``.

        Args:
            *registries (Self): Registries to merge, applied left to right.
            override (bool): Allow later registries to replace duplicate keys/selectors
                instead of raising. Defaults to ``False``.
            cross_registry (bool): Allow a patch to reach another registry's operations
                instead of raising. Defaults to ``False`` (logged when allowed).

        Returns:
            Self: A single registry combining all inputs.

        Raises:
            CoreException: On a duplicate key/selector without *override*, or on
                cross-registry patch reach without *cross_registry*.
        """

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
            override=override,
            cross_registry=cross_registry,
        )

        return cls(
            handlers=merged.handlers,
            plans=merged.plans,
            descriptors=merged.descriptors,
            patches=merged.patches,
        )

    # ....................... #

    @merge.instancemethod
    def _merge_instance(  # type: ignore[misc, override]
        self: Self,
        *registries: Self,
        override: bool = False,
        cross_registry: bool = False,
    ) -> Self:
        """Merge this registry with others — the instance form of :meth:`merge`.

        Args:
            *registries (Self): Other registries to merge with this one.
            override (bool): Allow later registries to replace duplicate keys/selectors.
                Defaults to ``False``.
            cross_registry (bool): Allow a patch to reach another registry's operations.
                Defaults to ``False`` (logged when allowed).

        Returns:
            Self: A single registry combining all inputs.

        Raises:
            CoreException: On a duplicate key/selector without *override*, or on
                cross-registry patch reach without *cross_registry*.
        """

        return type(self).merge(
            self, *registries, override=override, cross_registry=cross_registry
        )

    # ....................... #

    def freeze(self) -> FrozenOperationRegistry:
        """Convert the mutable registry into an immutable, execution-ready form.

        Resolves plan patches into per-operation plans and runs the freeze-time
        validator before snapshotting.

        Returns:
            FrozenOperationRegistry: The validated, immutable registry.

        Raises:
            CoreException: If freeze-time validation rejects the resolved plans.
        """

        resolution = self._resolution()
        RegistryFreezeValidator.validate_all(self._handlers, resolution)

        frozen_handlers = dict(self._handlers)
        frozen_plans = {
            op: resolution.resolve(str(op)).freeze() for op in frozen_handlers
        }

        # Visibility, not an error: descriptor-less operations stay executable but are
        # invisible/half-visible to catalog-driven surfaces (generated routes refuse
        # them, MCP tools lose schema + description). Internal-only operations may be
        # descriptor-less legitimately, so this is a single INFO line.
        if missing_descriptors := sorted(
            str(op) for op in frozen_handlers if op not in self._descriptors
        ):
            logger.info(
                "Operation registry frozen with operations lacking catalog "
                "descriptors (invisible to catalog-driven surfaces): %s",
                missing_descriptors,
            )

        return FrozenOperationRegistry(
            handlers=frozen_handlers,
            plans=frozen_plans,
            descriptors=dict(self._descriptors),
        )


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class FrozenOperationRegistry:
    """Frozen operation registry."""

    handlers: StrKeyMapping[OperationHandlerFactory] = attrs.field(
        factory=dict[StrKey, OperationHandlerFactory],
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

        Entries also carry plan-derived facts computed at freeze:
        ``supports_idempotency_key`` (the plan has an idempotency wrap — optional-key
        replay, not a requirement), ``required_permissions`` (union of permission
        keys declared by the plan's authz hooks; declared-hook introspection, not a
        security statement), ``requires_authn`` (the plan declares it needs a bound
        principal — an authn guard or any authz hook), and ``deadline`` (the plan's
        merged per-invocation time budget, or ``None`` for no cap).
        """

        return {
            op: OperationCatalogEntry(
                op=op,
                kind=self.plans[op].kind,
                descriptor=self.descriptors.get(op),
                supports_idempotency_key=self.plans[op].supports_idempotency_key,
                required_permissions=self.plans[op].required_permissions,
                requires_authn=self.plans[op].requires_authn,
                deadline=self.plans[op].deadline,
            )
            for op in self.handlers
        }

    # ....................... #

    @staticmethod
    def _operation_shape(entry: OperationCatalogEntry) -> dict[str, Any]:
        descriptor = entry.descriptor
        return {
            "kind": str(entry.kind),
            "input": descriptor.input_schema() if descriptor is not None else None,
            "output": descriptor.output_schema() if descriptor is not None else None,
            "tags": sorted(descriptor.tags) if descriptor is not None else [],
            "sensitive": descriptor.sensitive if descriptor is not None else False,
            "supports_idempotency_key": entry.supports_idempotency_key,
            "requires_authn": entry.requires_authn,
            "required_permissions": sorted(entry.required_permissions),
            "deadline_s": (
                entry.deadline.total_seconds() if entry.deadline is not None else None
            ),
        }

    def operation_fingerprint(self, op: StrKey) -> str:
        """Stable structural fingerprint of a single operation's contract + plan facts.

        Covers the operation's kind, input/output JSON schema, declared idempotency /
        authn / authz-permission / deadline facts, and catalog tags. Intended as a
        version tag: if it changes, the operation's observable contract changed.

        It is *structural*, not behavioral — it does NOT hash handler code or a hook's
        internal configuration, so a same-shape change to logic or middleware tuning is
        invisible. Treat a *differing* fingerprint as "cannot be trusted to reproduce"
        and a *matching* one as "same contract, probably reproducible".
        """

        return stable_payload_fingerprint(self._operation_shape(self.catalog()[op]))

    def fingerprint(self) -> str:
        """Structural fingerprint of the whole operation catalog (see :meth:`operation_fingerprint`)."""

        shape = {
            str(op): self._operation_shape(entry)
            for op, entry in sorted(self.catalog().items(), key=lambda kv: str(kv[0]))
        }
        return stable_payload_fingerprint(shape)

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

        # Build the handler under the read-only flag for a QUERY operation, so a factory
        # that *eagerly* acquires a command (write) port — the common kit pattern,
        # ``lambda ctx: Handler(port=ctx.document.command(spec))`` — hits the same
        # write-port guard that a call-time acquisition would. QUERY-ness is a static
        # property of the operation, so this is consistent with the resolve-once cache
        # (a QUERY op's handler is always built read-only). Without it the guard was
        # inert for eager acquisition, since the read-only flag is otherwise only set
        # later, inside ``ResolvedOperation.__call__``.
        if resolved_plan.kind is OperationKind.QUERY:
            with ctx.inv_ctx.bind_read_only():
                built = handler(ctx)
        else:
            built = handler(ctx)

        # A two-phase plan needs a two-phase handler (prepare/apply). The two
        # factory protocols are structurally identical (both ``__call__(ctx)``), so
        # this can't be caught at freeze without building the handler — surface a
        # clear error here (once per resolve) instead of an opaque AttributeError
        # deep in execution.
        if resolved_plan.two_phase and not (
            callable(getattr(built, "prepare", None))
            and callable(getattr(built, "apply", None))
        ):
            raise exc.configuration(
                f"Operation {op!r} is marked two-phase (.two_phase()) but its handler "
                f"{type(built).__name__} is not a TwoPhaseHandler — it has no "
                "prepare/apply. Register a TwoPhaseHandler (e.g. via "
                "TwoPhaseDocumentBuilder) or drop .two_phase().",
            )

        resolved = ResolvedOperation(
            op=op,
            handler=built,
            plan=resolved_plan,
            tx_runner=ctx.tx_ctx.scope,
            defer_after_commit=ctx.tx_ctx.run_or_defer,
            inv_ctx=ctx.inv_ctx,
            drain_gate=ctx.drain_gate,
        )

        ctx.store_operation(op, resolved)

        return resolved
