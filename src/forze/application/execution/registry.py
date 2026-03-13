"""Registry for usecase factories and composition plans.

Provides :class:`UsecaseRegistry` to register operation-to-factory mappings
and attach :class:`UsecasePlan` middleware composition. :meth:`resolve` builds
a fully composed usecase for an operation.
"""

import logging
from typing import Any, Callable, Literal, Optional, Self, final, overload

import attrs

from forze.base.errors import CoreError
from forze.base.logging import log_section

from .context import ExecutionContext
from .plan import OpKey, UsecasePlan
from .usecase import Usecase

# ----------------------- #

logger = logging.getLogger(__name__)

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

        logger.debug(
            "Registering usecase factory for operation %s (inplace=%s, factory_id=%s)",
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

        logger.debug(
            "Overriding usecase factory for operation %s (inplace=%s, factory_id=%s)",
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

        logger.debug(
            "Registering %d usecase factory(s) (inplace=%s)",
            len(ops),
            inplace,
        )

        with log_section():
            logger.debug("Operations: %s", tuple(ops.keys()))

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

        logger.debug(
            "Overriding %d usecase factory(s) (inplace=%s)",
            len(ops),
            inplace,
        )

        with log_section():
            logger.debug("Operations: %s", tuple(ops.keys()))

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

        logger.debug(
            "Extending usecase registry plan (inplace=%s, extra_ops=%d)",
            inplace,
            len(extra.ops),
        )

        with log_section():
            merged = UsecasePlan.merge(self.__plan, extra)
            logger.debug("Merged plan contains %d operation(s)", len(merged.ops))

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

    def resolve(
        self,
        op: OpKey,
        ctx: ExecutionContext,
        *,
        debug_plan: bool = False,
    ) -> Usecase[Any, Any]:
        """Build a fully composed usecase for an operation.

        Looks up the factory, optionally prints the plan when debug_plan,
        then delegates to :meth:`UsecasePlan.resolve`.

        :param op: Operation key.
        :param ctx: Execution context.
        :param debug_plan: When ``True``, print the middleware chain to stdout.
        :returns: Composed usecase with middlewares.
        :raises CoreError: If op is not registered.
        """
        op = str(op)

        logger.debug(
            "Resolving usecase for operation '%s' (debug_plan=%s)",
            op,
            debug_plan,
        )

        with log_section():
            factory = self.defaults.get(op)

            if not factory:
                raise CoreError(
                    f"Usecase factory is not registered for operation: {op}"
                )

            logger.debug(
                "Found factory for operation '%s' (factory_id=%s)", op, id(factory)
            )

            if debug_plan:
                logger.debug("Generating plan explanation for operation %s", op)
                explain = self.__plan.explain(op)
                print(explain.pretty_format())

            resolved = self.__plan.resolve(op, ctx, factory)

            logger.debug(
                "Resolved usecase for operation '%s' -> %s",
                op,
                type(resolved).__qualname__,
            )

        return resolved
