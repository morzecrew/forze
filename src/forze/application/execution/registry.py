"""Registry for usecase factories and composition plans.

Provides :class:`UsecaseRegistry` to register operation-to-factory mappings
and attach :class:`UsecasePlan` middleware composition. :meth:`resolve` builds
a fully composed usecase for an operation.
"""

from typing import Any, Literal, Self, cast, final, overload

import attrs
from structlog.contextvars import bind_contextvars

from forze.application._logger import logger
from forze.base.descriptors import hybridmethod
from forze.base.errors import CoreError

from .capabilities import SchedulableCapabilitySpec, schedule_capability_specs
from .context import ExecutionContext
from .dispatch import (
    assert_dispatch_edges_reference_registered_ops,
    assert_dispatch_graph_acyclic,
    expand_wildcard_dispatch_sources,
)
from .plan import (
    CAPABILITY_SCHEDULER_BUCKETS,
    WILDCARD,
    OpKey,
    UsecasePlan,
    middleware_specs_for_usecase_tuple,
)
from .usecase import Usecase, UsecaseFactory

# ----------------------- #


@final
@attrs.define(slots=True)
class UsecaseRegistry:
    """Container for registering usecase factories and composition plans.

    Maps operation keys to factories. Plan (middlewares, tx) is merged via
    :meth:`extend_plan`. :meth:`resolve` builds the composed usecase for an
    operation using the plan.
    """

    defaults: dict[str, UsecaseFactory] = attrs.field(factory=dict)
    """Operation key to factory mapping."""

    # Non initable fields
    _plan: UsecasePlan = attrs.field(factory=UsecasePlan, init=False, repr=False)
    """Composition plan for middleware and transaction wrapping."""

    _registry_id: str | None = attrs.field(default=None, init=False, repr=False)
    """The id of the registry."""

    _finalized: bool = attrs.field(default=False, init=False, repr=False)
    """Whether the registry is finalized and immutable."""

    _dispatch_edges: frozenset[tuple[str, str]] = attrs.field(
        factory=frozenset,
        init=False,
        repr=False,
    )
    """Directed edges ``(from_op, to_op)`` for static dispatch cycle validation."""

    strict_capability_middleware_without_engine: bool = attrs.field(default=False)
    """When ``True``, :meth:`finalize` rejects plans that set capability metadata while the
    merged :class:`UsecasePlan` disables :attr:`~forze.application.execution.plan.UsecasePlan.use_capability_engine`.
    """

    # ....................... #

    def _raise_if_finalized(self) -> None:
        """Raise an error if the registry is finalized."""

        if self._finalized:
            raise CoreError("Registry is finalized")

    # ....................... #

    def _validate_dispatch_graph_for_finalize(self) -> None:
        """Ensure declared and plan-derived dispatch edges form a DAG."""

        derived = self._plan.derived_dispatch_edges()
        combined = self._dispatch_edges | derived
        registered = set(self.defaults.keys())
        expanded = expand_wildcard_dispatch_sources(
            combined,
            registered,
            wildcard=WILDCARD,
        )
        assert_dispatch_graph_acyclic(expanded)
        assert_dispatch_edges_reference_registered_ops(expanded, registered)

    def _validate_capability_plans_for_finalize(self) -> None:
        """Run the capability scheduler on merged plans when the engine is enabled."""

        plan = self._plan

        if plan.use_capability_engine:
            for op in sorted(self.defaults.keys()):
                merged = plan.merged_operation_plan(op)
                merged.validate()

                for bucket in CAPABILITY_SCHEDULER_BUCKETS:
                    specs = middleware_specs_for_usecase_tuple(merged, bucket)
                    schedule_capability_specs(
                        cast(tuple[SchedulableCapabilitySpec, ...], specs),
                        bucket=bucket,
                    )

            return

        if not self.strict_capability_middleware_without_engine:
            return

        for op in sorted(self.defaults.keys()):
            merged = plan.merged_operation_plan(op)
            merged.validate()

            for bucket in CAPABILITY_SCHEDULER_BUCKETS:
                specs = middleware_specs_for_usecase_tuple(merged, bucket)

                for spec in specs:
                    if spec.requires or spec.provides:
                        raise CoreError(
                            f"Operation {op!r} declares capability requires/provides in bucket "
                            f"{bucket!r} but `UsecasePlan.use_capability_engine` is disabled. "
                            "Enable the capability engine or remove capability metadata from specs."
                        )

    # ....................... #

    @overload
    def finalize(self, registry_id: str, *, inplace: Literal[True]) -> None:
        """Finalize the registry and set the registry id.

        :param registry_id: The id of the registry.
        :raises CoreError: If the registry is already finalized.
        """
        ...

    @overload
    def finalize(self, registry_id: str, *, inplace: Literal[False] = False) -> Self:
        """Finalize the registry and set the registry id.

        :param registry_id: The id of the registry.
        :raises CoreError: If the registry is already finalized.
        :returns: New registry instance.
        """
        ...

    def finalize(self, registry_id: str, *, inplace: bool = False) -> Self | None:
        """Finalize the registry and set the registry id.

        :param registry_id: The id of the registry.
        :raises CoreError: If the registry is already finalized.
        """

        self._raise_if_finalized()

        if not registry_id:
            raise CoreError("Registry id cannot be empty")

        self._validate_dispatch_graph_for_finalize()
        self._validate_capability_plans_for_finalize()

        if inplace:
            self._finalized = True
            self._registry_id = registry_id
            return None

        else:
            new = attrs.evolve(self)
            new._finalized = True
            new._registry_id = registry_id
            new._plan = self._plan
            new._dispatch_edges = self._dispatch_edges

            return new

    # ....................... #

    def qualify_operation(self, op: OpKey) -> str:
        """Resolve an operation id for an operation.

        :param op: The operation to resolve the id for.
        :returns: The operation id.
        :raises CoreError: If the registry id is not set.
        """

        if not self._registry_id:
            raise CoreError("Registry id is not set")

        return f"{self._registry_id}.{op}"

    # ....................... #

    @overload
    def add_dispatch_edge(
        self,
        from_op: OpKey,
        to_op: OpKey,
        *,
        inplace: Literal[True],
    ) -> None:
        """Record a dispatch edge and mutate the registry in place."""
        ...

    @overload
    def add_dispatch_edge(
        self,
        from_op: OpKey,
        to_op: OpKey,
        *,
        inplace: Literal[False] = False,
    ) -> Self:
        """Record a dispatch edge and return a new registry."""
        ...

    def add_dispatch_edge(
        self,
        from_op: OpKey,
        to_op: OpKey,
        *,
        inplace: bool = False,
    ) -> Self | None:
        """Declare that ``from_op`` may synchronously dispatch ``to_op`` (e.g. via an effect).

        Used for static cycle detection at :meth:`finalize`. Runtime re-entrancy
        is still guarded by :meth:`ExecutionContext.push_usecase_dispatch`.

        :param from_op: Parent logical operation key.
        :param to_op: Child logical operation key.
        :param inplace: When ``True``, mutate this registry.
        """

        self._raise_if_finalized()

        edge = (str(from_op), str(to_op))
        new_edges = self._dispatch_edges | frozenset({edge})

        logger.trace(
            "Adding dispatch edge %s -> %s (inplace=%s)",
            edge[0],
            edge[1],
            inplace,
        )

        if inplace:
            self._dispatch_edges = new_edges
            return None

        new = attrs.evolve(self)
        new._dispatch_edges = new_edges

        return new

    # ....................... #

    @overload
    def register(
        self,
        op: OpKey,
        factory: UsecaseFactory,
        *,
        inplace: Literal[True],
    ) -> None:
        """Register a usecase factory and mutate the registry in place."""
        ...

    @overload
    def register(
        self,
        op: OpKey,
        factory: UsecaseFactory,
        *,
        inplace: Literal[False] = False,
    ) -> Self:
        """Register a usecase factory and return a new registry."""
        ...

    def register(
        self,
        op: OpKey,
        factory: UsecaseFactory,
        *,
        inplace: bool = False,
    ) -> Self | None:
        """Register a usecase factory for an operation.

        :param op: Logical operation name.
        :param factory: Factory that builds the usecase.
        :param inplace: When ``True``, mutate the registry in place, otherwise
            return a new instance.
        :raises CoreError: If a factory is already registered for ``op``.
        """

        self._raise_if_finalized()

        op = str(op)

        logger.trace(
            "Registering usecase factory for operation '%s' (inplace=%s, factory_id=%s)",
            op,
            inplace,
            id(factory),
        )

        if op in self.defaults:
            raise CoreError(
                f"Usecase factory is already registered for operation: {op}"
            )

        new = dict(self.defaults)
        new[op] = factory

        if inplace:
            self.defaults = new
            return None

        else:
            return attrs.evolve(self, defaults=new)

    # ....................... #

    @overload
    def override(
        self,
        op: OpKey,
        factory: UsecaseFactory,
        *,
        inplace: Literal[True],
    ) -> None:
        """Override an existing factory and mutate in place."""
        ...

    @overload
    def override(
        self,
        op: OpKey,
        factory: UsecaseFactory,
        *,
        inplace: Literal[False] = False,
    ) -> Self:
        """Override an existing factory and return a new registry."""
        ...

    def override(
        self,
        op: OpKey,
        factory: UsecaseFactory,
        *,
        inplace: bool = False,
    ) -> Self | None:
        """Override an existing usecase factory for an operation.

        The override is tracked so that conflicting :class:`UsecasePlan`
        overrides can be detected when plans are extended.

        :param op: Logical operation name to override.
        :param factory: Replacement factory.
        :param inplace: When ``True``, mutate the registry in place.
        :raises CoreError: If ``op`` has not been registered yet.
        """

        self._raise_if_finalized()

        op = str(op)

        logger.trace(
            "Overriding usecase factory for operation '%s' (inplace=%s, factory_id=%s)",
            op,
            inplace,
            id(factory),
        )

        if op not in self.defaults:
            raise CoreError(f"Usecase factory is not registered for operation: {op}")

        new = dict(self.defaults)
        new[op] = factory

        if inplace:
            self.defaults = new
            return None

        else:
            return attrs.evolve(self, defaults=new)

    # ....................... #

    @overload
    def register_many(
        self,
        ops: dict[OpKey, UsecaseFactory],
        *,
        inplace: Literal[True],
    ) -> None:
        """Register several operations at once.

        :param ops: Mapping from operation name to factory.
        :param inplace: When ``True``, mutate the registry in place.
        :raises CoreError: When any of the operations is already registered.
        """
        ...

    @overload
    def register_many(
        self,
        ops: dict[OpKey, UsecaseFactory],
        *,
        inplace: Literal[False] = False,
    ) -> Self:
        """Register several operations at once.

        :param ops: Mapping from operation name to factory.
        :param inplace: When ``True``, mutate the registry in place.
        :raises CoreError: When any of the operations is already registered.
        """
        ...

    def register_many(
        self,
        ops: dict[OpKey, UsecaseFactory],
        *,
        inplace: bool = False,
    ) -> Self | None:
        """Register several operations at once.

        :param ops: Mapping from operation name to factory.
        :param inplace: When ``True``, mutate the registry in place.
        :raises CoreError: When any of the operations is already registered.
        """

        self._raise_if_finalized()

        ops = {str(op): factory for op, factory in ops.items()}

        logger.trace(
            "Registering %s usecase factory(s) (inplace=%s)",
            len(ops),
            inplace,
        )

        logger.trace("Operations: %s", tuple(ops.keys()))

        already_registered = set(self.defaults.keys()).intersection(ops.keys())

        if already_registered:
            raise CoreError(
                f"Usecase factories are already registered for operations: {already_registered}"
            )

        new = dict(self.defaults)
        new.update(ops)

        if inplace:
            self.defaults = new
            return None

        else:
            return attrs.evolve(self, defaults=new)

    # ....................... #

    @overload
    def override_many(
        self,
        ops: dict[OpKey, UsecaseFactory],
        *,
        inplace: Literal[True],
    ) -> None:
        """Override several operations in a single call.

        :param ops: Mapping from operation name to replacement factory.
        :param inplace: When ``True``, mutate the registry in place.
        :raises CoreError: When any of the operations has not yet been
            registered.
        """
        ...

    @overload
    def override_many(
        self,
        ops: dict[OpKey, UsecaseFactory],
        *,
        inplace: Literal[False] = False,
    ) -> Self:
        """Override several operations in a single call.

        :param ops: Mapping from operation name to replacement factory.
        :param inplace: When ``True``, mutate the registry in place.
        :raises CoreError: When any of the operations has not yet been
            registered.
        """
        ...

    def override_many(
        self,
        ops: dict[OpKey, UsecaseFactory],
        *,
        inplace: bool = False,
    ) -> Self | None:
        """Override several operations in a single call.

        :param ops: Mapping from operation name to replacement factory.
        :param inplace: When ``True``, mutate the registry in place.
        :raises CoreError: When any of the operations has not yet been
            registered.
        """

        self._raise_if_finalized()

        ops = {str(op): factory for op, factory in ops.items()}

        logger.trace(
            "Overriding %s usecase factory(s) (inplace=%s)",
            len(ops),
            inplace,
        )

        logger.trace("Operations: %s", tuple(ops.keys()))

        not_yet_registered = set(ops.keys()).difference(self.defaults.keys())

        if not_yet_registered:
            raise CoreError(
                f"Usecase factories are not registered for operations: {not_yet_registered}"
            )

        new = dict(self.defaults)
        new.update(ops)

        if inplace:
            self.defaults = new
            return None

        else:
            return attrs.evolve(self, defaults=new)

    # ....................... #

    @overload
    def extend_plan(
        self,
        extra: UsecasePlan,
        *,
        inplace: Literal[True],
    ) -> None:
        """Attach additional planning information to the registry.

        :param extra: Plan to merge into the existing registry plan.
        :param inplace: When ``True``, mutate the registry in place.
        """

        ...

    @overload
    def extend_plan(
        self,
        extra: UsecasePlan,
        *,
        inplace: Literal[False] = False,
    ) -> Self:
        """Attach additional planning information to the registry.

        :param extra: Plan to merge into the existing registry plan.
        :param inplace: When ``True``, mutate the registry in place.
        """

        ...

    def extend_plan(
        self,
        extra: UsecasePlan,
        *,
        inplace: bool = False,
    ) -> Self | None:
        """Attach additional planning information to the registry.

        :param extra: Plan to merge into the existing registry plan.
        :param inplace: When ``True``, mutate the registry in place.
        """

        self._raise_if_finalized()

        logger.trace(
            "Extending usecase registry plan (inplace=%s, extra_ops=%s)",
            inplace,
            len(extra.ops),
        )

        merged = self._plan.merge(extra)

        if inplace:
            self._plan = merged
            return None

        else:
            new = attrs.evolve(self)
            new._plan = merged

            return new

    # ....................... #

    def exists(self, op: OpKey) -> bool:
        """Return ``True`` when a factory is registered for ``op``."""

        op = str(op)

        return op in self.defaults

    # ....................... #

    def resolve(self, op: OpKey, ctx: ExecutionContext) -> Usecase[Any, Any]:
        """Build a fully composed usecase for an operation.

        Looks up the factory,then delegates to :meth:`UsecasePlan.resolve`.

        :param op: Operation key.
        :param ctx: Execution context.
        :returns: Composed usecase with middlewares.
        :raises CoreError: If op is not registered.
        """

        op = str(op)
        operation_id = self.qualify_operation(op)

        bind_contextvars(operation_id=operation_id)

        logger.debug("Resolving usecase")
        factory = self.defaults.get(op)

        if not factory:
            raise CoreError(f"Usecase factory is not registered for operation: {op}")

        logger.trace("Found factory (factory_id=%s)", id(factory))
        resolved = self._plan.resolve(op, ctx, factory)
        assigned = resolved.with_operation_id(operation_id)

        return assigned

    # ....................... #

    @hybridmethod
    def merge(  # type: ignore[misc]
        cls: type[Self],  # pyright: ignore[reportGeneralTypeIssues]
        *registries: Self,
        on_conflict: Literal["error", "overwrite"] = "error",
    ) -> Self:
        """Merge multiple registries into a single registry.

        If method called on an instance, the instance is merged with the other registries.
        Otherwise only provided registries are merged.

        :param registries: Registries to merge.
        :param on_conflict: What to do when a factory is registered for the same operation in multiple registries.
        :returns: New registry with all factories merged.
        :raises CoreError: If a factory is registered for the same operation in multiple registries and ``on_conflict`` is ``"error"``.
        """

        for reg in registries:
            try:
                reg._raise_if_finalized()

            except CoreError as e:
                raise CoreError("Cannot merge finalized registry") from e

        logger.trace(
            "Merging %s usecase registries (on_conflict=%s)",
            len(registries),
            on_conflict,
        )

        acc = cls()

        if not registries:
            logger.trace("No registries provided, returning empty registry")
            return acc

        for idx, reg in enumerate(registries, 1):
            logger.trace(
                "Processing registry #%s (factory_count=%s, plan_ops=%s, registry_id=%s)",
                idx,
                len(reg.defaults),
                len(reg._plan.ops),
                id(reg),
            )

            # Merge factories
            for op, factory in reg.defaults.items():
                existing = acc.defaults.get(op)

                if existing is not None:
                    logger.trace(
                        "Conflict for operation '%s' (existing_factory_id=%s, new_factory_id=%s)",
                        op,
                        id(existing),
                        id(factory),
                    )

                    if on_conflict == "error":
                        raise CoreError(
                            f"Usecase factory is already registered for operation: {op}"
                        )

                    logger.trace(
                        "Overwriting factory for operation '%s' with rightmost registry entry",
                        op,
                    )

                else:
                    logger.trace(
                        "Adding factory for operation '%s' (factory_id=%s)",
                        op,
                        id(factory),
                    )

                acc.defaults[op] = factory

            # Merge plans
            logger.trace(
                "Merging plan from registry #%s (ops=%s)",
                idx,
                len(reg._plan.ops),
            )
            acc.extend_plan(reg._plan, inplace=True)

        logger.trace(
            "Merged %s registries into one (factory_count=%s, plan_ops=%s)",
            len(registries),
            len(acc.defaults),
            len(acc._plan.ops),
        )

        acc._dispatch_edges = frozenset(
            {
                *acc._dispatch_edges,
                *[e for reg in registries for e in reg._dispatch_edges],
            }
        )

        acc.strict_capability_middleware_without_engine = any(
            reg.strict_capability_middleware_without_engine for reg in registries
        )

        return acc

    # ....................... #

    @merge.instancemethod
    def _merge_instance(  # pyright: ignore[reportUnusedFunction]
        self: Self,
        *registries: Self,
        on_conflict: Literal["error", "overwrite"] = "error",
    ) -> Self:
        """Merge this registry with another registry.

        :param registries: Registries to merge into this registry.
        :param on_conflict: What to do when a factory is registered for the same operation in multiple registries.
        :returns: New registry with all factories merged.
        :raises CoreError: If a factory is registered for the same operation in multiple registries and ``on_conflict`` is ``"error"``.
        """

        return type(self).merge(self, *registries, on_conflict=on_conflict)
