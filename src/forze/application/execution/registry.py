"""Registry for application usecase factories.

The registry owns a mapping from logical operation names to
callables that produce concrete :class:`~forze.application.execution.usecase.Usecase`
instances. It also tracks an associated :class:`~forze.application.execution.plan.UsecasePlan`
that describes how guards and effects should wrap each operation.
"""

from enum import StrEnum
from typing import (
    Any,
    Callable,
    Literal,
    Optional,
    Self,
    TypeVar,
    cast,
    final,
    get_origin,
    overload,
)

import attrs

from forze.base.errors import CoreError

from .context import ExecutionContext
from .plan import UsecasePlan
from .usecase import Usecase

# ----------------------- #

U = TypeVar("U", bound=Usecase[Any, Any])

UsecaseFactory = Callable[[ExecutionContext], U]
"""Factory that builds a concrete :class:`Usecase` from a :class:`ExecutionContext`."""

OpKey = str | StrEnum
"""Key for operation names."""


def _op(op: OpKey) -> str:
    return str(op)


# ....................... #


@final
@attrs.define(slots=True)
class UsecaseRegistry:
    """Container for registering and composing usecases.

    The registry is responsible for:

    * registering factories for named operations
    * applying overrides in a controlled way
    * attaching a :class:`UsecasePlan` that enriches usecases with guards
      and effects

    It can be used in an immutable style by returning new instances, or in a
    mutable style when ``inplace=True`` is passed to mutation methods.
    """

    defaults: dict[str, UsecaseFactory[Any]] = attrs.field(factory=dict)

    # Non initable fields
    __plan: UsecasePlan = attrs.field(factory=UsecasePlan, init=False, repr=False)
    __overriden: frozenset[str] = attrs.field(factory=frozenset, init=False, repr=False)

    # ....................... #

    @overload
    def register(
        self,
        op: OpKey,
        factory: UsecaseFactory[Any],
        *,
        inplace: Literal[True],
    ) -> None:
        """Register a usecase factory for an operation.

        :param op: Logical operation name.
        :param factory: Factory that builds the usecase.
        :param inplace: When ``True``, mutate the registry in place, otherwise
            return a new instance.
        :raises CoreError: If a factory is already registered for ``op``.
        """
        ...

    @overload
    def register(
        self,
        op: OpKey,
        factory: UsecaseFactory[Any],
        *,
        inplace: Literal[False] = False,
    ) -> Self:
        """Register a usecase factory for an operation.

        :param op: Logical operation name.
        :param factory: Factory that builds the usecase.
        :param inplace: When ``True``, mutate the registry in place, otherwise
            return a new instance.
        :raises CoreError: If a factory is already registered for ``op``.
        """
        ...

    def register(
        self,
        op: OpKey,
        factory: UsecaseFactory[Any],
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

        op = _op(op)

        if op in self.defaults:
            raise CoreError(
                f"Usecase factory is already registered for operation: {op}"
            )

        new = dict(self.defaults)
        new[op] = factory

        if inplace:
            self.defaults = new
            return

        else:
            new_instance = type(self)(defaults=new)
            return new_instance

    # ....................... #

    @overload
    def override(
        self,
        op: OpKey,
        factory: UsecaseFactory[Any],
        *,
        inplace: Literal[True],
    ) -> None:
        """Override an existing usecase factory for an operation.

        The override is tracked so that conflicting :class:`UsecasePlan`
        overrides can be detected when plans are extended.

        :param op: Logical operation name to override.
        :param factory: Replacement factory.
        :param inplace: When ``True``, mutate the registry in place.
        :raises CoreError: If ``op`` has not been registered yet.
        """
        ...

    @overload
    def override(
        self,
        op: OpKey,
        factory: UsecaseFactory[Any],
        *,
        inplace: Literal[False] = False,
    ) -> Self:
        """Override an existing usecase factory for an operation.

        The override is tracked so that conflicting :class:`UsecasePlan`
        overrides can be detected when plans are extended.

        :param op: Logical operation name to override.
        :param factory: Replacement factory.
        :param inplace: When ``True``, mutate the registry in place.
        :raises CoreError: If ``op`` has not been registered yet.
        """

    def override(
        self,
        op: OpKey,
        factory: UsecaseFactory[Any],
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

        op = _op(op)

        if op not in self.defaults:
            raise CoreError(f"Usecase factory is not registered for operation: {op}")

        new = dict(self.defaults)
        new[op] = factory

        if inplace:
            self.defaults = new
            self.__overriden = frozenset({*self.__overriden, op})
            return

        else:
            new_instance = type(self)(defaults=new)
            new_instance.__overriden = frozenset({*self.__overriden, op})
            return new_instance

    # ....................... #

    @overload
    def register_many(
        self,
        ops: dict[OpKey, UsecaseFactory[Any]],
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
        ops: dict[OpKey, UsecaseFactory[Any]],
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
        ops: dict[OpKey, UsecaseFactory[Any]],
        *,
        inplace: bool = False,
    ) -> Optional[Self]:
        """Register several operations at once.

        :param ops: Mapping from operation name to factory.
        :param inplace: When ``True``, mutate the registry in place.
        :raises CoreError: When any of the operations is already registered.
        """

        ops = {_op(op): factory for op, factory in ops.items()}

        already_registered = set(self.defaults.keys()).intersection(ops.keys())

        if already_registered:
            raise CoreError(
                f"Usecase factories are already registered for operations: {already_registered}"
            )

        new = dict(self.defaults)
        new.update(ops)

        if inplace:
            self.defaults = new
            return

        else:
            new_instance = type(self)(defaults=new)
            return new_instance

    # ....................... #

    @overload
    def override_many(
        self,
        ops: dict[OpKey, UsecaseFactory[Any]],
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
        ops: dict[OpKey, UsecaseFactory[Any]],
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
        ops: dict[OpKey, UsecaseFactory[Any]],
        *,
        inplace: bool = False,
    ) -> Optional[Self]:
        """Override several operations in a single call.

        :param ops: Mapping from operation name to replacement factory.
        :param inplace: When ``True``, mutate the registry in place.
        :raises CoreError: When any of the operations has not yet been
            registered.
        """

        ops = {_op(op): factory for op, factory in ops.items()}

        not_yet_registered = set(ops.keys()).difference(self.defaults.keys())

        if not_yet_registered:
            raise CoreError(
                f"Usecase factories are not registered for operations: {not_yet_registered}"
            )

        new = dict(self.defaults)
        new.update(ops)

        if inplace:
            self.defaults = new
            self.__overriden = frozenset({*self.__overriden, *ops.keys()})
            return

        else:
            new_instance = type(self)(defaults=new)
            new_instance.__overriden = frozenset({*self.__overriden, *ops.keys()})
            return new_instance

    # ....................... #

    @overload
    def extend_plan(
        self,
        extra: UsecasePlan,
        *,
        inplace: Literal[True],
        allow_override_on_overriden: bool = False,
    ) -> None:
        """Attach additional planning information to the registry.

        :param extra: Plan to merge into the existing registry plan.
        :param inplace: When ``True``, mutate the registry in place.
        :param allow_override_on_overriden: When ``True``, allow the plan to
            override operations that have also been overridden at the registry
            level. When ``False``, such conflicts raise :class:`CoreError`.
        """

        ...

    @overload
    def extend_plan(
        self,
        extra: UsecasePlan,
        *,
        inplace: Literal[False] = False,
        allow_override_on_overriden: bool = False,
    ) -> Self:
        """Attach additional planning information to the registry.

        :param extra: Plan to merge into the existing registry plan.
        :param inplace: When ``True``, mutate the registry in place.
        :param allow_override_on_overriden: When ``True``, allow the plan to
            override operations that have also been overridden at the registry
            level. When ``False``, such conflicts raise :class:`CoreError`.
        """

        ...

    def extend_plan(
        self,
        extra: UsecasePlan,
        *,
        inplace: bool = False,
        allow_override_on_overriden: bool = False,
    ) -> Optional[Self]:
        """Attach additional planning information to the registry.

        :param extra: Plan to merge into the existing registry plan.
        :param inplace: When ``True``, mutate the registry in place.
        :param allow_override_on_overriden: When ``True``, allow the plan to
            override operations that have also been overridden at the registry
            level. When ``False``, such conflicts raise :class:`CoreError`.
        :raises CoreError: When a plan tries to override an operation that has
            already been explicitly overridden and
            ``allow_override_on_overriden`` is ``False``.
        """

        if not allow_override_on_overriden:
            for op, pl in extra.ops.items():
                if pl.override is not None and op in self.__overriden:
                    raise CoreError(
                        f"Plan override for '{op}' conflicts with registry override. "
                        "Use allow_override_on_overridden=True explicitly."
                    )

        merged = UsecasePlan.merge(self.__plan, extra)

        if inplace:
            self.__plan = merged
            return

        else:
            new_instance = type(self)(defaults=self.defaults)
            new_instance.__plan = merged

            return new_instance

    # ....................... #

    def exists(self, op: OpKey) -> bool:
        """Return ``True`` when a factory is registered for ``op``."""

        op = _op(op)

        return op in self.defaults

    # ....................... #

    def resolve(
        self,
        op: OpKey,
        ctx: ExecutionContext,
        *,
        expected: Optional[type[U]] = None,
    ) -> U:
        """Build a fully composed usecase for an operation.

        The method looks up the factory for ``op``, resolves the configured
        :class:`UsecasePlan`, and optionally validates that the concrete
        instance is of the expected type.

        :param op: Logical operation name.
        :param ctx: Execution context passed to the underlying factories.
        :param expected: Expected concrete type of the resulting usecase.
        :returns: A composed :class:`Usecase` instance.
        :raises CoreError: When no factory is registered or the resulting
            usecase has an unexpected type.
        """

        op = _op(op)
        factory = self.defaults.get(op)

        if not factory:
            raise CoreError(f"Usecase factory is not registered for operation: {op}")

        uc = self.__plan.resolve(op, ctx, cast(UsecaseFactory[U], factory))

        if expected is not None:
            check_type = get_origin(expected) or expected
            if not isinstance(uc, check_type):
                raise CoreError(f"Usecase '{op}' has unexpected type: {type(uc)!r}")

        return uc

    #! TODO: add resolve_tx method separately !
