"""Registry for usecase factories and composition plans.

Provides :class:`UsecaseRegistry` to register operation-to-factory mappings
and attach :class:`UsecasePlan` middleware composition. :meth:`resolve` builds
a fully composed usecase for an operation.
"""

from typing import Any, Callable, Literal, Optional, Self, final, overload

import attrs

from forze.base.descriptors import hybridmethod
from forze.base.errors import CoreError
from forze.base.logging import getLogger

from .context import ExecutionContext
from .plan import OpKey, UsecasePlan
from .usecase import Usecase

# ----------------------- #

logger = getLogger(__name__).bind(scope="registry")

# ....................... #

UsecaseFactory = Callable[[ExecutionContext], Usecase[Any, Any]]
"""Factory that builds a usecase from execution context."""

# ....................... #


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
    __plan: UsecasePlan = attrs.field(factory=UsecasePlan, init=False, repr=False)
    """Composition plan for middleware and transaction wrapping."""

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
    ) -> Optional[Self]:
        """Register a usecase factory for an operation.

        :param op: Logical operation name.
        :param factory: Factory that builds the usecase.
        :param inplace: When ``True``, mutate the registry in place, otherwise
            return a new instance.
        :raises CoreError: If a factory is already registered for ``op``.
        """

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
            new_instance = type(self)(defaults=new)
            return new_instance

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
    ) -> Optional[Self]:
        """Override an existing usecase factory for an operation.

        The override is tracked so that conflicting :class:`UsecasePlan`
        overrides can be detected when plans are extended.

        :param op: Logical operation name to override.
        :param factory: Replacement factory.
        :param inplace: When ``True``, mutate the registry in place.
        :raises CoreError: If ``op`` has not been registered yet.
        """

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
            new_instance = type(self)(defaults=new)
            return new_instance

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
    ) -> Optional[Self]:
        """Register several operations at once.

        :param ops: Mapping from operation name to factory.
        :param inplace: When ``True``, mutate the registry in place.
        :raises CoreError: When any of the operations is already registered.
        """

        ops = {str(op): factory for op, factory in ops.items()}

        logger.trace(
            "Registering %d usecase factory(s) (inplace=%s)",
            len(ops),
            inplace,
        )

        with logger.section():
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
                new_instance = type(self)(defaults=new)
                return new_instance

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
    ) -> Optional[Self]:
        """Override several operations in a single call.

        :param ops: Mapping from operation name to replacement factory.
        :param inplace: When ``True``, mutate the registry in place.
        :raises CoreError: When any of the operations has not yet been
            registered.
        """

        ops = {str(op): factory for op, factory in ops.items()}

        logger.trace(
            "Overriding %d usecase factory(s) (inplace=%s)",
            len(ops),
            inplace,
        )

        with logger.section():
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
                new_instance = type(self)(defaults=new)
                return new_instance

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
    ) -> Optional[Self]:
        """Attach additional planning information to the registry.

        :param extra: Plan to merge into the existing registry plan.
        :param inplace: When ``True``, mutate the registry in place.
        """

        logger.trace(
            "Extending usecase registry plan (inplace=%s, extra_ops=%d)",
            inplace,
            len(extra.ops),
        )

        merged = self.__plan.merge(extra)

        if inplace:
            self.__plan = merged
            return None

        else:
            new_instance = type(self)(defaults=self.defaults)
            new_instance.__plan = merged
            return new_instance

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

        logger.debug("Resolving usecase for operation '%s'", op)
        factory = self.defaults.get(op)

        with logger.section():
            if not factory:
                raise CoreError(
                    f"Usecase factory is not registered for operation: {op}"
                )

            logger.trace("Found factory (factory_id=%s)", id(factory))

        resolved = self.__plan.resolve(op, ctx, factory)

        return resolved

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

        logger.trace(
            "Merging %d usecase registries (on_conflict=%s)",
            len(registries),
            on_conflict,
        )

        acc = cls()

        with logger.section():
            if not registries:
                logger.trace("No registries provided, returning empty registry")
                return acc

            for idx, reg in enumerate(registries, 1):
                logger.trace(
                    "Processing registry #%d (factory_count=%d, plan_ops=%d, registry_id=%s)",
                    idx,
                    len(reg.defaults),
                    len(reg.__plan.ops),
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
                    "Merging plan from registry #%d (ops=%d)",
                    idx,
                    len(reg.__plan.ops),
                )
                acc.extend_plan(reg.__plan, inplace=True)

            logger.trace(
                "Merged %d registries into one (factory_count=%d, plan_ops=%d)",
                len(registries),
                len(acc.defaults),
                len(acc.__plan.ops),
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
